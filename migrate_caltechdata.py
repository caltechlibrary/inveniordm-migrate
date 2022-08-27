import os
import json, csv
import s3fs
import sys
import requests
from caltechdata_api import decustomize_schema, caltechdata_write
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar
from py_dataset import dataset

# Load affiliation mapping globally so we only do this once
AFF_MAPPING = {}
AFF_NAME = {}
with open("fixed_affiliations.csv", "r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)
    for row in reader:
        a = row["affiliation"].strip('"')
        AFF_MAPPING[a] = row["ROR_Affiliation"]
        AFF_NAME[a] = row["ROR_Name"]
AFF_SPLIT = {}
with open("split_affiliations.csv", "r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)
    for row in reader:
        a = row["affiliation"].strip('"')
        AFF_SPLIT[a] = row


def check_identifiers(identifiers):
    cd_id = None
    doi = None
    cleaned = []
    for idv in identifiers:
        typev = idv["identifierType"]
        if typev == "CaltechDATA_Identifier":
            if not cd_id:
                cd_id = idv["identifier"]
                cleaned.append(idv)
            else:
                print("Multiple CD identifiers, something is wrong")
                exit()
        elif typev == "DOI":
            if not doi:
                doi = idv["identifier"]
                cleaned.append(idv)
            else:
                print("Multiple DOI identifiers, something is wrong")
                exit()
        else:
            print(f"Identifier {typev} not transferring")
    if not cd_id:
        print("NO CD identifiers, something is wrong")
        exit()
    if not doi:
        print("NO DOI identifiers, something is wrong")
        exit()
    return cd_id, doi, cleaned


def clean_subjects(subjects):
    cleaned = []
    for sub in subjects:
        s = sub["subject"]
        if "," in s:
            split = s.split(",")
            for subject in split:
                cleaned.append({"subject": subject.strip()})
        elif ";" in s:
            split = s.split(";")
            for subject in split:
                cleaned.append({"subject": subject.strip()})
        else:
            cleaned.append({"subject": s.strip()})
    return cleaned


def identify_records(subjects):
    sub = []
    for s in subjects:
        sub.append(s["subject"])
    if "gps" in sub and "thesis" in sub:
        return "GPS_Thesis"


def grid_to_ror(grid):
    # We manually handle some incorrect/redundant GRID Ids
    if grid == "grid.451078.f":
        ror = "https://ror.org/00hm6j694"
    else:
        url = f"https://api.ror.org/organizations?query.advanced=external_ids.GRID.all:{grid}"
        results = requests.get(url)
        ror = results.json()["items"][0]["id"]
    return ror


def clean_person(person):
    # For creators or contributors
    name = person["name"]
    if "," in name:
        split = name.split(",")
        person["familyName"] = split[0]
        person["givenName"] = split[1:]
        person["nameType"] = "Personal"
    else:
        person["nameType"] = "Organizational"
    if "nameIdentifiers" in person:
        identifiers = []
        for identifier in person["nameIdentifiers"]:
            drop = False
            if "nameIdentifierScheme" in identifier:
                if identifier["nameIdentifierScheme"] == "GRID":
                    identifier["nameIdentifier"] = grid_to_ror(
                        identifier["nameIdentifier"]
                    )
                    identifier["nameIdentifierScheme"] = "ROR"
                if identifier["nameIdentifierScheme"] == "researcherid":
                    drop = True
            if drop == False:
                identifiers.append(identifier)
        person["nameIdentifiers"] = identifiers
    if "affiliation" in person:
        full = []
        for aff in person["affiliation"]:
            a_name = aff["name"].strip()
            if a_name in AFF_SPLIT:
                # We need to split an affiliation
                mapping = AFF_SPLIT[a_name]
                full.append(
                    {
                        "name": mapping["first_affiliation"],
                        "affiliationIdentifier": mapping["first_ROR"],
                        "affiliationIdentifierScheme": "ROR",
                    }
                )
                if mapping["second_affiliation"] != "":
                    full.append(
                        {
                            "name": mapping["second_affiliation"],
                            "affiliationIdentifier": mapping["second_ROR"],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
                if mapping["third_affiliation"] != "":
                    full.append(
                        {
                            "name": mapping["third_affiliation"],
                            "affiliationIdentifier": mapping["third_ROR"],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
                if mapping["fourth_affiliation"] != "":
                    full.append(
                        {
                            "name": mapping["fourth_affiliation"],
                            "affiliationIdentifier": mapping["fourth_ROR"],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
                if mapping["fifth_affiliation"] != "":
                    full.append(
                        {
                            "name": mapping["fifth_affiliation"],
                            "affiliationIdentifier": mapping["fifth_ROR"],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
                if mapping["sixth_affiliation"] != "":
                    full.append(
                        {
                            "name": mapping["sixth_affiliation"],
                            "affiliationIdentifier": mapping["sixth_ROR"],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
            elif a_name in AFF_MAPPING:
                # We just add a ROR
                if AFF_MAPPING[a_name] == "":
                    full.append({"name": a_name})
                elif a_name == "Caltech":
                    full.append(
                        {
                            "name": a_name,
                            "affiliationIdentifier": AFF_MAPPING[a_name],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
                elif a_name != AFF_NAME[a_name]:
                    # Temporarily dropping RORs where the name doesn't match
                    # Since we can't currently control the affiliation name
                    print("NOT MAPPING THIS ROR:")
                    print(a_name, AFF_NAME[a_name])
                    full.append({"name": a_name})
                else:
                    full.append(
                        {
                            "name": a_name,
                            "affiliationIdentifier": AFF_MAPPING[a_name],
                            "affiliationIdentifierScheme": "ROR",
                        }
                    )
            else:
                print(f"{aff} is missing")
                exit()
        print(full)
        person["affiliation"] = full

    return person


def write_record(metadata, files, s3):
    identifiers = metadata["identifiers"]
    cd_id, doi, identifiers = check_identifiers(identifiers)
    metadata["identifiers"] = identifiers
    # metadata["id"] = cd_id

    # Separate family and given names
    for creator in metadata["creators"]:
        creator = clean_person(creator)
    if "contributors" in metadata:
        for c in metadata["contributors"]:
            c = clean_person(c)
    descriptions = []
    abstract = False
    for description in metadata["descriptions"]:
        if description["descriptionType"] == "Abstract":
            abstract = True
        if description["description"].startswith("<br>Cite this record as:") == False:
            descriptions.append(description)
    if abstract == False:
        # We always want an abstract, if not set pick the first description
        descriptions[0]["descriptionType"] = "Abstract"
    metadata["descriptions"] = descriptions
    if "subjects" in metadata:
        subjects = metadata["subjects"]
        subjects = clean_subjects(subjects)
        identity = identify_records(subjects)
        if identity == "GPS_Thesis":
            metadata["types"]["resourceType"] = "Map"
    # Clean up dates with commas in them
    dates = []
    for date in metadata["dates"]:
        if "," in date["date"]:
            split = date["date"].split(",")
            for s in split:
                print(s)
                dates.append({"date": s.strip(), "dateType": date["dateType"]})
        else:
            dates.append(date)
    metadata["dates"] = dates
    if "fundingReferences" in metadata:
        for f in metadata["fundingReferences"]:
            if "funderIdentifierType" in f:
                if f["funderIdentifierType"] == "GRID":
                    f["funderIdentifier"] = grid_to_ror(f["funderIdentifier"])
                    f["funderIdentifierType"] = "ROR"
    idv = caltechdata_write(
        metadata,
        schema="43",
        pilot=True,
        files=files,
        publish=True,
        production=True,
        s3=s3,
    )
    return cd_id, idv


# Large file code
# <a role=\"button\" class=\"ui compact mini button\" href=\"https://renc.osn.xsede.org/ini210004tommorrell/0_D1.20077/Proximal_Colon/S14_02.czi\">\n<i class=\"download icon\"></i>\nDownload\n</a>

bucket = "caltechdata-backup"
path = "caltechdata"
s3 = s3fs.S3FileSystem()
records = s3.ls(f"{bucket}/{path}")
size = 0
with open("new_ids.json", "r") as infile:
    record_ids = json.load(infile)
with open("large_records.json", "r") as infile:
    large = json.load(infile)
    for l in large:
        record_ids[l] = "large"
for record in records:
    if "10.22002" not in record:
        idv = record.split("caltechdata/")[1]
        if idv not in record_ids:
            print(idv)
            files = s3.ls(record)
            upload = []
            for f in files:
                if "datacite.json" not in f and "raw.json" not in f:
                    upload.append(f)
                    size += s3.info(f)["Size"]
            with s3.open(f"{record}/datacite.json", "r") as j:
                metadata = json.load(j)
                cd_id, new_id = write_record(metadata, upload, s3)
                record_ids[cd_id] = new_id
                with open("new_ids.json", "w") as outfile:
                    json.dump(record_ids, outfile)
            print("Total Size: ", size / (10**9))
