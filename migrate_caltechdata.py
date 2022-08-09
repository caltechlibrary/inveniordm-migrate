import os
import json
import s3fs
import sys
import requests
from caltechdata_api import decustomize_schema, caltechdata_write
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar
from py_dataset import dataset


def check_identifiers(identifiers):
    cd_id = None
    doi = None
    for idv in identifiers:
        typev = idv["identifierType"]
        if typev == "CaltechDATA_Identifier":
            if not cd_id:
                cd_id = idv["identifier"]
            else:
                print("Multiple CD identifiers, something is wrong")
                exit()
        if typev == "DOI":
            if not doi:
                doi = idv["identifier"]
            else:
                print("Multiple DOI identifiers, something is wrong")
                exit()
    if not cd_id:
        print("NO CD identifiers, something is wrong")
        exit()
    if not doi:
        print("NO DOI identifiers, something is wrong")
        exit()
    return cd_id, doi


def write_record(metadata, files, s3):
    identifiers = metadata.pop("identifiers")
    cd_id, doi = check_identifiers(identifiers)
    metadata["id"] = cd_id
    metadata["pids"] = {
        "doi": {"identifier": doi, "provider": "datacite", "client": "caltech.library"}
    }
    # Separate family and given names
    for creator in metadata["creators"]:
        name = creator["name"]
        if "," in name:
            split = name.split(",")
            creator["familyName"] = split[0]
            creator["givenName"] = split[1]
            creator["nameYype"] = "Personal"
        else:
            creator["nameType"] = "Organizational"
    idv = caltechdata_write(
        metadata, schema="43", pilot=True, files=files, publish=False,
        production=True, s3=s3
    )
    return cd_id, idv


bucket = "caltechdata-backup"
path = "caltechdata"
s3 = s3fs.S3FileSystem()
records = s3.ls(f"{bucket}/{path}")
size = 0
record_ids = {}
for record in records:
    print(record)
    if "10.22002" not in record:
        files = s3.ls(record)
        upload = []
        for f in files:
            if "datacite.json" not in f and 'raw.json' not in f:
                upload.append(f)
                size += s3.info(f)["Size"]
        with s3.open(f"{record}/datacite.json", "r") as j:
            metadata = json.load(j)
            cd_id, new_id = write_record(metadata, upload, s3)
            record_ids[cd_id] = new_id
            with open("new_ids.json", "w") as outfile:
                json.dump(record_ids, outfile)
        print('Total Size: ',size/(10**9))
