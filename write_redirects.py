import json

with open("new_ids.json", "r") as infile:
    record_ids = json.load(infile)

with open("redirect-map.conf","w") as outfile:
    outfile.write("map $request_uri $redirect_uri {\n")
    for record in record_ids:
        new_id = record_ids[record]
        if new_id != 'large':
            s = f'    /records/{record}          /records/{new_id};\n'
            outfile.write(s)
    outfile.write("}")

