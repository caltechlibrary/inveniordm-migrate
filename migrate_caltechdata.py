import os
import json, csv
import s3fs
import sys
import requests
from caltechdata_api import decustomize_schema, caltechdata_write
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta
from progressbar import progressbar

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
FUNDER_MAPPING = {}
FUNDER_NAME = {}
with open("funder_RORs.csv", "r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)
    for row in reader:
        a = row["affiliation"].strip('"')
        FUNDER_MAPPING[a] = row["ROR_Affiliation"]
        FUNDER_NAME[a] = row["organizationLookupName_Affiliation"]


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
    elif grid == 'grid.5805.8':
        ror = "https://ror.org/02en5vm52"
    else:
        url = f"https://api.ror.org/organizations?query.advanced=external_ids.GRID.all:{grid}"
        results = requests.get(url)
        ror = results.json()["items"][0]["id"]
    return ror


def add_affiliation(full, affiliation, ror):
    if affiliation != "":
        if ror != "":
            full.append(
                {
                    "name": affiliation,
                    "affiliationIdentifier": ror,
                    "affiliationIdentifierScheme": "ROR",
                }
            )
        else:
            full.append(
                {
                    "name": affiliation,
                }
            )
    return full


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
            #Handle incomplete TCCON records
            if 'grid' in identifier["nameIdentifier"]:
                identifier["nameIdentifier"] = grid_to_ror(
                        identifier["nameIdentifier"]
                    )
                identifier["nameIdentifierScheme"] = "ROR"
            elif "nameIdentifierScheme" in identifier:
                if identifier["nameIdentifierScheme"] == "GRID":
                    identifier["nameIdentifier"] = grid_to_ror(
                        identifier["nameIdentifier"]
                    )
                    identifier["nameIdentifierScheme"] = "ROR"
                if identifier["nameIdentifierScheme"] == "researcherid":
                    drop = True
                if identifier["nameIdentifierScheme"] == "ResearcherID":
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
                full = add_affiliation(
                    full, mapping["first_affiliation"], mapping["first_ROR"]
                )
                full = add_affiliation(
                    full, mapping["second_affiliation"], mapping["second_ROR"]
                )
                full = add_affiliation(
                    full, mapping["third_affiliation"], mapping["third_ROR"]
                )
                full = add_affiliation(
                    full, mapping["fourth_affiliation"], mapping["fourth_ROR"]
                )
                full = add_affiliation(
                    full, mapping["fifth_affiliation"], mapping["fifth_ROR"]
                )
                full = add_affiliation(
                    full, mapping["sixth_affiliation"], mapping["sixth_ROR"]
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
        person["affiliation"] = full

    return person


def write_record(metadata, files, s3, file_links):
    identifiers = metadata["identifiers"]
    cd_id, doi, identifiers = check_identifiers(identifiers)
    metadata["identifiers"] = identifiers
    # metadata["id"] = cd_id

    contributor_string = ""
    # Separate family and given names
    for creator in metadata["creators"]:
        creator = clean_person(creator)
    if "contributors" in metadata:
        for c in metadata["contributors"]:
            c = clean_person(c)
            if "contributorEmail" in c:
                contributor_string = (
                    f"Contact person: {c['name']} {c['contributorEmail']}"
                )
    descriptions = []
    abstract = False
    for description in metadata["descriptions"]:
        #description['description'] = description['description'].replace('\n','</p><p>')
        if description["descriptionType"] == "Abstract":
            abstract = True
        if description["description"].startswith("<br>Cite this record as:") == False:
            if description["description"].startswith("<br>Unique Views:") == False:
                descriptions.append(description)
    if abstract == False:
        # We always want an abstract, if not set pick the first description
        descriptions[0]["descriptionType"] = "Abstract"
    # Other metadata that we're currently puting in descriptions field
    if "publications" in metadata:
        pub = metadata.pop("publications")
        outstring = "<p>Related Publication:&lt;/p&gt;</p>"
        if "publicationTitle" in pub:
            outstring += f'<p>{pub["publicationTitle"]}&lt;/p&gt;</p>'
        if "publicationAuthors" in pub:
            for author in pub["publicationAuthors"]:
                if "publicationAuthorAffiliation" in author:
                    outstring += f"<p>{author['publicationAuthorName']} {author['publicationAuthorAffiliation']}&lt;/p&gt;</p>"
                else:
                    outstring += f"<p>{author['publicationAuthorName']}&lt;/p&gt;</p>"
        if "publicationPublisher" in pub:
            outstring += f"<p>{pub['publicationPublisher']}&lt;/p&gt;</p>"
        if "publicationPublicationDate" in pub:
            outstring += f"<p>{pub['publicationPublicationDate']}&lt;/p&gt;</p>"
        if "publicationIDs" in pub:
            pub_doi = pub["publicationIDs"]["publicationIDNumber"]
            outstring += f"<p>https://doi.org/{pub_doi}&lt;/p&gt;</p>"
        if "publicationLanguage" in pub:
            outstring += f"{pub['publicationLanguage']}"
        descriptions.append({"description": outstring, "descriptionType": "Other"})
    if contributor_string != "":
        descriptions.append(
            {"description": contributor_string, "descriptionType": "Other"}
        )
    metadata["descriptions"] = descriptions
    if "subjects" in metadata:
        subjects = metadata["subjects"]
        metadata["subjects"] = clean_subjects(subjects)
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
            if "funderName" in f:
                name = f['funderName']
                if name in FUNDER_MAPPING:
                    if FUNDER_NAME[name] == name:
                        if FUNDER_MAPPING[name] != '':
                            f["funderIdentifier"] = FUNDER_MAPPING[name]
                            f["funderIdentifierType"] = "ROR"
                    else:
                        print(f'NOT MAPPING FUNDER {name} {FUNDER_NAME[name]}')
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
        file_links = file_links,
        s3=s3,
    )
    return cd_id, idv

bucket = "caltechdata-backup"
path = "caltechdata"
s3 = s3fs.S3FileSystem()
records = s3.ls(f"{bucket}/{path}")
size = 0
with open("new_ids.json", "r") as infile:
    record_ids = json.load(infile)
large = []
with open("large_records.json", "r") as infile:
    largen = json.load(infile)
    for l in largen:
        large.append(l)
        record_ids[l] = "large"
# Read in site id file with CaltechDATA IDs
with open("tccon_active.csv") as infile:
    site_ids = csv.reader(infile)
    for row in site_ids:
        record_ids[row[1]] = "tccon"
with open("osn_active.csv") as infile:
    site_ids = csv.reader(infile)
    for row in site_ids:
        record_ids[row[0]] = "osn"
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
                    #size += s3.info(f)["Size"]
            with s3.open(f"{record}/datacite.json", "r") as j:
                metadata = json.load(j)
                if idv in large:
                    file_links = []
                    for file in upload:
                        name = file.split('/')[-1]
                        link = f'https://renc.osn.xsede.org/ini210004tommorrell/D1.{idv}/{name}'
                        file_links.append(link)
                    cd_id, new_id = write_record(metadata, [], s3, file_links)
                else:
                    cd_id, new_id = write_record(metadata, upload, s3, [])
            record_ids[cd_id] = new_id
            with open("new_ids.json", "w") as outfile:
                json.dump(record_ids, outfile)
            #print("Total Size: ", size / (10**9))
