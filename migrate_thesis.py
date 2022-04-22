import os
import json
import random
import requests
from caltechdata_api import decustomize_schema, caltechdata_write
from caltech_thesis import epxml_to_datacite
from epxml_support import download_records
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar
import getpass
import xmltodict


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


def write_records(fnames):
    for eprintf in fnames:
        # FIX
        files = []

        # write eprint thesis record to InvenioRDM
        with open(f"{eprintf}.xml", encoding="utf8") as fd:
            eprint = xmltodict.parse(fd.read())["eprints"]["eprint"]
        metadata = epxml_to_datacite(eprint)
        # Need to figure out identifiers
        metadata.pop("identifiers")
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
        if "funding" in metadata:
            funding = metadata["funding"]
            for fund in funding:
                if "award" in fund:
                    if name not in fund["award"]:
                        fund["award"]["name"] = "None"
        metadata["types"]["resourceType"] = "Thesis"

        print(metadata)

        doi = caltechdata_write(
            metadata, schema="43", pilot=True, files=files, publish=True
        )
        print(doi)


random_list = random.sample(range(1, 12000), k=5)

r_user = input("Enter your CaltechTHESIS username: ")
r_pass = getpass.getpass()

good_list = []

for r in random_list:
    status = requests.get(f"https://thesis.library.caltech.edu/{r}").status_code
    if status == 200:
        good_list.append(r)
print(good_list)

eprintf = download_records(good_list, "thesis", r_user, r_pass)

write_records(good_list)
