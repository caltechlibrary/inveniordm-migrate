import os
import json, csv
import s3fs
import sys

bucket = "caltechdata-backup"
path = "caltechdata"
s3 = s3fs.S3FileSystem()
records = s3.ls(f"{bucket}/{path}")
size = 0
with open("new_ids.json", "r") as infile:
    record_ids = json.load(infile)
tccon = []
with open("tccon_active.csv") as infile:
    site_ids = csv.reader(infile)
    for row in site_ids:
        tccon.append(row[1])
osn = []
with open("osn_active.csv") as infile:
    site_ids = csv.reader(infile)
    for row in site_ids:
        osn.append(row[0])
with open('owners.csv','w') as outfile:
    writer = csv.writer(outfile)
    for record in records:
        if "10.22002" not in record:
            idv = record.split("caltechdata/")[1]
            if idv not in osn:
                with s3.open(f"{record}/raw.json", "r") as j:
                    metadata = json.load(j)
                    if 'owners' in metadata['metadata']:
                        owner = metadata['metadata']['owners'][0]
                        if owner != 2:
                            writer.writerow([record_ids[idv],owner])
                    else:
                        print(metadata)
