import os
import json
import sys
import requests
from urllib.parse import urlparse
from subprocess import run, Popen, PIPE
from datetime import datetime, timedelta

from py_dataset import dataset

def ep_full(c_name, eprint_url, eprint_username, eprint_password):
    cmd = ['ep']
    cmd.append('-dataset')
    cmd.append(c_name)
    cmd.append('-api')
    cmd.append(eprint_url)
    if eprint_username != '' and eprint_password != '':
        cmd.append('-auth')
        cmd.append('basic')
        cmd.append('-username')
        cmd.append(eprint_username)
        cmd.append('-password')
        cmd.append(eprint_password)
    cmd.append('-export')
    cmd.append('all')
    p = run(cmd)
    exit_code = p.returncode
    if exit_code != 0:
        print(f"ERROR: {' '.join(cmd)}, exit code {exit_code}")
        sys.exit(1)

c_name = 'oh.ds'

ok = dataset.status(c_name)
if ok == False:
    err = dataset.init(c_name, layout = "pairtree")
    if err != '':
        print(f'{c_name}, {err}')

harvest = False

if harvest == True:
    username = os.environ["USER"]
    password = os.environ["PW"]
    returnc = ep_full(c_name,'http://oralhistories.library.caltech.edu/',username,password)
    print(returnc)

keys = dataset.keys(c_name)
for key in keys:
    existing,err = dataset.read(c_name,key)
    print(existing)
    new = {"_access": {
        "metadata_restricted": "false",
        "files_restricted": "false"
        },
        "_owners": [1],
        "_created_by": 1,
        "access_right": "open",
        "resource_type": {
            "type":"text",
            "subtype":"oral_history"
        }}
    new['titles'] = [{'title':existing['title']}]
    crea = []
    for creator in existing['creators']['items']:
        cre = {'name':creator['name']['given']+' '+creator['name']['family'],
                'type':'Personal'}
        crea.append(cre)
    new['creators'] = crea
    new['publication-date'] = existing['datestamp']
    headers = {"Content-Type": "application/json"}
    url = 'https://localhost/api/records'
    response = requests.post(url,headers=headers,json=new,verify=False)
    print(response)
    exit()
