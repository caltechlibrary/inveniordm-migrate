import os
import json
import sys
import requests
from caltechdata_api import decustomize_schema
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar
from py_dataset import dataset
from invenio_rdm_records.resources.serializers import DataCite43JSONSerializer


def download_file(url, fname):
    r = requests.get(url, stream=True)
    if os.path.isfile(fname):
        print("Using already downloaded file")
        return fname
    elif r.status_code == 403:
        print(
            "It looks like this file is embargoed.  We can't access until after the embargo is lifted"
        )
        return None
    else:
        with open(fname, "wb") as f:
            total_length = int(r.headers.get("content-length"))
            for chunk in progressbar(
                r.iter_content(chunk_size=1024), max_value=(total_length / 1024) + 1
            ):
                if chunk:
                    f.write(chunk)
                    # f.flush()
        return fname


def read_records(data):
    # read records in 'hits' structure
    for record in data:
        rid = str(record["id"])
        metadata = record["metadata"]
        files = []
        if "electronic_location_and_access" in metadata:
            for erecord in metadata["electronic_location_and_access"]:
                url = erecord["uniform_resource_identifier"]
                fname = erecord["electronic_name"][0]
                f = download_file(url, fname)
                if f != None:
                    files.append(f)
        else:
            print("No Files")
        metadata = decustomize_schema(
            metadata, pass_emails=True, pass_owner=True, schema="43"
        )
        print(metadata)
        serializer = DataCite43JSONSerializer()
        metadata = serializer.load(metadata)
        print(metadata)
        exit()


api_url = "https://data.caltech.edu/api/records/"

# Get the existing records
data = requests.get(api_url).json()

read_records(data["hits"]["hits"])
# if we have more pages of data
while "next" in data["links"]:
    data = requests.get(data["links"]["next"]).json()

    read_records(data["hits"]["hits"])

    # print(existing)
    new = {
        "_access": {"metadata_restricted": False, "files_restricted": False},
        "_owners": [1],
        "_created_by": 1,
        "_default_preview": "previewer one",
        "access_right": "open",
        "resource_type": {"type": "publication", "subtype": "publication-other"},
    }
    new["recid"] = existing["eprint_id"]
    new["titles"] = [{"title": existing["title"], "type": "MainTitle"}]
    crea = []
    if "creators" in existing:
        for creator in existing["creators"]["items"]:
            cre = {
                "name": creator["name"]["given"] + " " + creator["name"]["family"],
                "type": "Personal",
            }
            crea.append(cre)
    if "corp_creators" in existing:
        for creator in existing["corp_creators"]["items"]:
            if creator == "California Institute of Technology":
                cre = {
                    "name": creator["name"],
                    "type": "Organizational",
                    "identifiers": {"ROR": "05dxps055"},
                }
            else:
                cre = {"name": creator["name"], "type": "Organizational"}
            crea.append(cre)
    new["creators"] = crea
    if "contributors" in existing:
        for contributor in existing["contributors"]["items"]:
            cont = {
                "name": contributor["name"]["given"]
                + " "
                + contributor["name"]["family"],
                "type": "Personal",
            }
            if contributor["type"] == "http://coda.library.caltech.edu/ARA":
                # Incorrect - Filler
                cont["role"] = "DataCurator"
            else:
                # Incorrect - Filler
                cont["role"] = "ProjectManager"
    new["language"] = "eng"
    new["publication_date"] = existing["date"]
    if "abstract" in existing:
        new["descriptions"] = [
            {"description": existing["abstract"], "type": "Abstract", "lang": "eng"}
        ]
    else:
        new["descriptions"] = [
            {
                "description": "No description available",
                "type": "Abstract",
                "lang": "eng",
            }
        ]
    # new["community"] = {
    # "primary": "Caltech Oral Histories",
    # }
    new["licenses"] = [
        {
            "license": "Creative Commons Attribution NonCommercial",
            "uri": "https://creativecommons.org/licenses/by-nc/2.0/",
            "identifier": "CC-BY-NC",
            "scheme": "CC-BY-NC",
        }
    ]
    if "keywords" in existing:
        subjects = []
        keywords = existing["keywords"].split(",")
        for key in keywords:
            subjects.append({"subject": key, "identifier": key, "scheme": "no-scheme"})
    new["subjects"] = subjects
    new["identifiers"] = {"DOI": "10.9999/rdm.9999999"}
    pdf_url = existing["documents"][0]["files"][0]["url"]
    pdf_file = download_file(pdf_url)
    headers = {"Content-Type": "application/json"}
    url = "https://localhost:5000/api/records/"
    response = requests.post(url, headers=headers, json=new, verify=False)
    print(response.status_code)
    if response.status_code != 201:
        print(response.text)
    else:
        print("Record Created")
        idv = response.json()["id"]
        print(idv)
        headers = {"Content-Type": "application/octet-stream"}
        record_url = url + idv + "/files/" + pdf_file
        files = {"file": open(pdf_file, "rb")}
        response = requests.put(record_url, headers=headers, files=files, verify=False)
        print(response.status_code)
        if response.status_code != 200:
            print(response.text)
        else:
            print("File Attached")
            os.remove(pdf_file)
