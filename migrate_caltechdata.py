import os
import json
import sys
import requests
from caltechdata_api import decustomize_schema, caltechdata_write
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar
from py_dataset import dataset


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
        # Need to figure out identifiers
        metadata.pop("identifiers")
        # Separate family and given names
        for creator in metadata["creators"]:
            name = creator["name"]
            print(name)
            if "," in name:
                print("Yes")
                split = name.split(",")
                creator["familyName"] = split[0]
                creator["givenName"] = split[1]
                creator["nameYype"] = "Personal"
            else:
                creator["nameType"] = "Organizational"
        print(metadata)
        doi = caltechdata_write(
            metadata, schema="43", pilot=True, files=files, publish=True
        )
        print(doi)


api_url = "https://data.caltech.edu/api/records/"

# Get the existing records
data = requests.get(api_url).json()

read_records(data["hits"]["hits"])
# if we have more pages of data
while "next" in data["links"]:
    data = requests.get(data["links"]["next"]).json()

    read_records(data["hits"]["hits"])
