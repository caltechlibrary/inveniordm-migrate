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
            print(f'Identifier {typev} not transferring')
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
        s = sub['subject']
        if ',' in s:
            split = s.split(',')
            for subject in split:
                cleaned.append({'subject':subject.strip()})
        elif ';' in s:
            split = s.split(';')
            for subject in split:
                cleaned.append({'subject':subject.strip()})
        else:
            cleaned.append({'subject':s.strip()})
    return cleaned

def identify_records(subjects):
    sub = []
    for s in subjects:
        sub.append(s['subject'])
    if 'gps' in sub and 'thesis' in sub:
        return 'GPS_Thesis'


def grid_to_ror(grid):
    url = f'https://api.ror.org/organizations?query.advanced=external_ids.GRID.all:{grid}'
    results = requests.get(url)
    ror = results.json()['items'][0]['id']
    return(ror)

def clean_person(person):
    #For creators or contributors
    name = person["name"]
    if "," in name:
        split = name.split(",")
        person["familyName"] = split[0]
        person["givenName"] = split[1:]
        person["nameYype"] = "Personal"
    else:
        person["nameType"] = "Organizational"
    if 'nameIdentifiers' in person:
        for identifier in person['nameIdentifiers']:
            if 'nameIdentifierScheme' in identifier:
                if identifier['nameIdentifierScheme'] == 'GRID':
                    identifier['nameIdentifier'] = grid_to_ror(identifier['nameIdentifier'])
                    identifier['nameIdentifierScheme'] = 'ROR'
    #if 'affiliations' in name:
    #    for aff in name['affiliations']:
    return person

def write_record(metadata, files, s3):
    identifiers = metadata["identifiers"]
    cd_id, doi, identifiers = check_identifiers(identifiers)
    metadata["identifiers"] = identifiers
    metadata["id"] = cd_id
    #metadata["identifiers"] = [
    #    {"identifier": doi, "identifierType": "DOI"}
    #    ]
    metadata["pids"] = {
        "doi": {"identifier": doi, "provider": "external"}
    }
    # Separate family and given names
    for creator in metadata["creators"]:
        creator = clean_person(creator)
    if 'contributors' in metadata:
        for c in metadata["contributors"]:
            c = clean_person(c)
    descriptions = []
    abstract = False
    for description in metadata['descriptions']:
        if description['descriptionType'] == 'Abstract':
            abstract = True
        if description['description'].startswith('<br>Cite this record as:')==False:
            descriptions.append(description)
    if abstract==False:
        #We always want an abstract, if not set pick the first description
        descriptions[0]['descriptionType'] = 'Abstract'
    metadata['descriptions'] = descriptions
    if 'subjects' in metadata:
        subjects = metadata['subjects'] 
        subjects = clean_subjects(subjects)
        identity = identify_records(subjects)
        if identity == 'GPS_Thesis':
            metadata['types']['resourceType'] = 'Map'
    #Clean up dates with commas in them
    dates = []
    for date in metadata['dates']:
        if ',' in date['date']:
            split = date['date'].split(',')
            for s in split:
                print(s)
                dates.append({"date":s.strip(),"dateType":date["dateType"]})
        else:
            dates.append(date)
    metadata['dates'] = dates
    idv = caltechdata_write(
        metadata, schema="43", pilot=True, files=files, publish=True,
        production=True, s3=s3
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
        record_ids[l] = 'large'
for record in records:
    if "10.22002" not in record:
        idv = record.split('caltechdata/')[1]
        if idv not in record_ids:
            print(idv)
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
