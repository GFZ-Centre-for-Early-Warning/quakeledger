#!/usr/bin/env python3

'''
Aim of this script is to add
event data from a csv file
to the database.
'''

import argparse
import sqlite3
import pandas as pd


class MapToAttribute():
    def __init__(self, attribute):
        self._attribute = attribute

    def __call__(self, old_series):
        return old_series[self._attribute]

class MapToNone():
    def __call__(self, old_series):
        return None

class MapToAttributeWithPrefix():
    def __init__(self, prefix, attribute):
        self._prefix = prefix
        self._attribute = attribute

    def __call__(self, old_series):
        return self._prefix + str(old_series[self._attribute])
        
MAPPINGS = {
    'eventID': MapToAttributeWithPrefix(prefix='peru_', attribute='rupid'),
    'Agency': MapToAttribute('Agency'),
    'Identifier': MapToAttributeWithPrefix(prefix='peru_', attribute='rupid'),
    'year': MapToAttribute('Unnamed: 12'),
    'month': MapToAttribute('Unnamed: 13'),
    'day': MapToAttribute('Unnamed: 14'),
    'minute': MapToNone(),
    'second': MapToNone(),
    'timeUncertainty': MapToNone(),
    'longitude': MapToAttribute('centroid_lon'),
    'longitudeUncertainty': MapToNone(),
    'latitide': MapToAttribute('centroid_lat'),
    'latitideUncertainty': MapToNone(),
    'horizontalUncertainty': MapToNone(),
    'minHorizontalUncertainty': MapToNone(),
    'maxHorizontalUncertainty': MapToNone(),
    'azimuthMaxHorizontalUncertainty': MapToNone(),
    'depth': MapToAttribute('centroid_depth'),
    'depthUncertainty': MapToNone(),
    'magnitude': MapToAttribute('mag'),
    'magnitudeUncertainty': MapToNone(),
    'rake': MapToAttribute('rake'),
    'rakeUncertainty': MapToNone(),
    'dip': MapToAttribute('dip'),
    'dipUncertainty': MapToNone(),
    'strike': MapToAttribute('strike'),
    'strikeUncertainty': MapToNone(),
    # we have historic in the new series,
    # but just observed in the old dataset
    # we can map them afterwards with sql
    # but I'm still a bit unsure about
    # the values here
    'type': MapToAttribute('type'),
    'probability': MapToNone(),
}

def map_to_existing_names(series):
    new_series = pd.Series()

    for key in MAPPINGS.keys():
        new_series[key] = MAPPINGS[key](series)
    return new_series

def no_comment_row(x_str):
    return x_str[0] != '#'

def insert_into_sqlite(series, con):
    data = tuple(series[[
        'eventID',
        'Agency',
        'Identifier',
        'year',
        'month',
        'day',
        'hour',
        'minute',
        'second',
        'timeUncertainty',
        'longitude',
        'longitudeUncertainty',
        'latitude',
        'latitudeUncertainty',
        'horizontalUncertainty',
        'minHorizontalUncertainty',
        'maxHorizontalUncertainty',
        'azimuthMaxHorizontalUncertainty',
        'depth',
        'depthUncertainty',
        'magnitude',
        'magnitudeUncertainty',
        'rake',
        'rakeUncertainty',
        'dip',
        'dipUncertainty',
        'strike',
        'strikeUncertainty',
        'type',
        'probability'
    ]].tolist())
    query_to_insert = '''
        INSERT INTO events (
            eventID,
            Agency,
            Identifier,
            year,
            month,
            day,
            hour,
            minute,
            second,
            timeUncertainty,
            longitude,
            longitudeUncertainty,
            latitude,
            latitudeUncertainty,
            horizontalUncertainty,
            minHorizontalUncertainty,
            maxHorizontalUncertainty,
            azimuthMaxHorizontalUncertainty,
            depth,
            depthUncertainty,
            magnitude,
            magnitudeUncertainty,
            rake,
            rakeUncertainty,
            dip,
            dipUncertainty,
            strike,
            strikeUncertainty,
            type,
            probability
        ) VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
    '''
    con.execute(query_to_insert, data)

def main():
    argparser = argparse.ArgumentParser(
        description='Inserts events to the sqlite database.',
    )
    argparser.add_argument(
        '--csvfile',
        help='Path to the csv file of which the input should be added.',
    )

    args = argparser.parse_args()

    con = sqlite3.connect('sqlite3.db')

    data = pd.read_csv(args.csvfile, skiprows=1)

    for index, row in data.iterrows():
        mapped_row = map_to_existing_names(row)

        insert_into_sqlite(mapped_row, con)

        if index % 1000 == 0:
            print('inserted {}'.format(mapped_row['eventID']))
            con.commit()
    con.commit()

if __name__ == '__main__':
    main()
