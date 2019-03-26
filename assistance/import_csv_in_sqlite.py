#!/usr/bin/env python3

'''
This script is to import a csv file into a sqlite database

Usage:

python3 import_csv_in_sqlite.py [csv-file] [sqlite-database] [name of the table]

Example:
python3 import_csv_in_sqlite.py sites.csv db.sqlite sites
'''

import argparse
import sqlite3
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description='''
    Imports a csv file into a sqlite database
    ''')

    parser.add_argument('csvfile', type=str)
    parser.add_argument('dbfile', type=str)
    parser.add_argument('tablename', type=str)

    args = parser.parse_args()

    data = pd.read_csv(args.csvfile)

    con = sqlite3.connect(args.dbfile)

    data.to_sql(args.tablename, con)


if __name__ == '__main__':
    main()

