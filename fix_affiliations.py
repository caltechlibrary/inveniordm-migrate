import csv

corrected = []
with open("ROR_Affiliations.tsv", "r") as file:
    reader = csv.DictReader(file, delimiter="\t")
    for row in reader:
        la = row["affiliation"].strip('"')
        corrected.append(la)
mapping = {}
ROR_names = {}
index = 0
with open("ROR_Affiliations.csv", "r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)
    for row in reader:
        mapping[corrected[index]] = row["ROR_Affiliation"]
        ROR_names[corrected[index]] = row["organizationLookupName_Affiliation"]
        index = index + 1
with open("fixed_affiliations.csv", "w", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["affiliation", "ROR_Affiliation", "ROR_Name"])
    for m in mapping:
        writer.writerow([m, mapping[m], ROR_names[m]])
