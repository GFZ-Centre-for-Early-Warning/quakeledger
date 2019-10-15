#Quakeledger

[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/gfzriesgos/quakeledger)](https://hub.docker.com/r/gfzriesgos/quakeledger)
[![Build Status](https://travis-ci.com/gfzriesgos/quakeledger.svg?branch=master)](https://travis-ci.com/gfzriesgos/quakeledger)

This is a rewrite from
https://github.com/GFZ-Centre-for-Early-Warning/quakeledger

and

https://github.com/bpross-52n/quakeledger


The main difference is that this code is refactored and uses a sqlite database.

The data imported are the valparaiso\_v1.3.csv file as events table,
the sites.csv as sites table and the
mean\_disagg.csv as mean\_disagg table.

## Requirements

Following python modules need to be installed:
- pandas
- scipy
- sqlalchemy
- lxml
- numpy

# Setup
You can use a virtual environment and the requirements.txt:

```shell
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

You also must make sure that you extract the sqlite database (which is zipped
here because of file size policies on Github).

```
unzip sqlite3.db.zip
```


You can make sure that the script works by running
```
python3 test_all.py
```
