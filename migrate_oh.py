import os
import json
import sys
import requests
from urllib.parse import urlparse
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar

from py_dataset import dataset


def download_file(url):
    fname = url.split("/")[-1]
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


def ep_full(c_name, eprint_url, eprint_username, eprint_password):
    cmd = ["ep"]
    cmd.append("-dataset")
    cmd.append(c_name)
    cmd.append("-api")
    cmd.append(eprint_url)
    if eprint_username != "" and eprint_password != "":
        cmd.append("-auth")
        cmd.append("basic")
        cmd.append("-username")
        cmd.append(eprint_username)
        cmd.append("-password")
        cmd.append(eprint_password)
    cmd.append("-export")
    cmd.append("all")
    p = run(cmd)
    exit_code = p.returncode
    if exit_code != 0:
        print(f"ERROR: {' '.join(cmd)}, exit code {exit_code}")
        sys.exit(1)


c_name = "oh.ds"

ok = dataset.status(c_name)
if ok == False:
    err = dataset.init(c_name, layout="pairtree")
    if err != "":
        print(f"{c_name}, {err}")

harvest = False

if harvest == True:
    username = os.environ["USER"]
    password = os.environ["PW"]
    returnc = ep_full(
        c_name, "http://oralhistories.library.caltech.edu/", username, password
    )
    print(returnc)

keys = dataset.keys(c_name)
for key in keys:
    existing, err = dataset.read(c_name, key)
    # print(existing)
    new = {
        "_access": {"metadata_restricted": False, "files_restricted": False},
        "_owners": [1],
        "_created_by": 1,
        "_default_preview": "previewer one",
        "access_right": "open",
        "resource_type": {"type": "publication", "subtype": "publication-other"},
    }
    new["titles"] = [{"title": existing["title"], "type": "MainTitle"}]
    crea = []
    for creator in existing["creators"]["items"]:
        cre = {
            "name": creator["name"]["given"] + " " + creator["name"]["family"],
            "type": "Personal",
            "identifiers": {"Orcid": "9999-9999-9999-9999"},
        }
        crea.append(cre)
    new["creators"] = crea
    new["language"] = "eng"
    new["publication_date"] = existing["datestamp"]
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
