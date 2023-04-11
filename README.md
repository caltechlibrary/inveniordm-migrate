Assorted scripts to migrate content to InvenioRDM and S3 data sources
=====================================================

This repo holds scripts user to migrate content into InvenioRDM. These have
generally been used for one-time migration activities, but may be useful in the
future.

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg?style=flat-square)](https://choosealicense.com/licenses/bsd-3-clause)
[![Latest release](https://img.shields.io/github/v/release/caltechlibrary/inveniordm-migrate.svg?style=flat-square&color=b44e88)](https://github.com/caltechlibrary/inveniordm-migrate/releases)


Table of contents
-----------------

* [Usage](#usage)
* [Getting help](#getting-help)
* [License](#license)
* [Authors and history](#authors-and-history)
* [Acknowledgments](#authors-and-acknowledgments)


Usage
-----


### CaltechDATA

`migrate_caltechdata.py` was usilized to move records from the TIND-managed
Invenio instance to InvenioRDM

### CaltechTHESIS

`migrate_caltechthesis.py` was utilized to creats some minimal test records in
InvenioRDM. It is not complete.

### OSN Migration

For large collections of data we sometimes need to move the data first, and
then create InvenioRDM records. An S3 object store like the Open Storage
Network is a great option. You can bulk move records efficiently with
[s5cmd](https://github.com/peak/s5cmd) and the management scripts.

Run `python make_command.py` to generate a list of files to sync. You'll need
to set environment variables with

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
S3_ENDPOINT_URL https://renc.osn.xsede.org
AWS_REGION us-east-1
```

Then run the command with 
`nohup ./s5cmd -numworkers 100 run commands.txt >> & log2017.txt ; echo Done >> & log2017.txt &`.
You may be able to adjust the numworkers component depending on the OS.


Getting help
------------

Raise an issue on the issue tacker.


License
-------

Software produced by the Caltech Library is Copyright (C) 2023, Caltech.  This software is freely distributed under a BSD/MIT type license.  Please see the [LICENSE](LICENSE) file for more information.


Authors and history
---------------------------

These scripts were written by Tom Morrell.

Acknowledgments
---------------

This work was funded by the California Institute of Technology Library.


<div align="center">
  <br>
  <a href="https://www.caltech.edu">
    <img width="100" height="100" src=".graphics/caltech-round.png">
  </a>
</div>
