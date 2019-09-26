#!/usr/bin/env python3

'''
Aim of this script is to add
event data from a csv file
to the database.
'''

import argparse
import os
import sqlite3

import pandas as pd


class MapToAttribute():
    '''
    Returns an attribute of the old series.
    '''
    def __init__(self, attribute):
        self._attribute = attribute

    def __call__(self, old_series):
        return old_series[self._attribute]


class MapToOneValue():
    '''
    Returns just one value.
    '''
    def __init__(self, value):
        self._value = value

    def __call__(self, old_series):
        return self._value


class MapToOneValueIfAttributeIsNone():
    '''
    Returns the value from the attribute or
    (if none) a default value.
    '''
    def __init__(self, attribute, value):
        self._attribute = attribute
        self._value = value

    def __call__(self, old_series):
        possible_value = old_series[self._attribute]
        if possible_value is not None:
            return possible_value
        return self._value


class MapToNone():
    '''
    Returns None.
    '''
    def __call__(self, old_series):
        return None


class MapToAttributeWithPrefix():
    '''
    Returns the attribute of a series with
    a prefix.
    The prefix is the same for all the values.
    '''
    def __init__(self, prefix, attribute):
        self._prefix = prefix
        self._attribute = attribute

    def __call__(self, old_series):
        return self._prefix + str(old_series[self._attribute])


class MapToAttributeAndMapValues():
    '''
    Returns the attribute of the series.
    If this has a specific value, it will
    be mapped to anohter value depending
    on the content of the map values.
    '''
    def __init__(self, attribute, lookup_table):
        self._attribute = attribute
        self._lookup_table = lookup_table

    def __call__(self, old_series):
        old_value = old_series[self._attribute]
        if old_value in self._lookup_table.keys():
            return self._lookup_table[old_value]
        return old_value


MAPPINGS = {
    'eventID': MapToAttributeWithPrefix(prefix='peru_', attribute='rupid'),
    'Agency': MapToAttribute('Agency'),
    'Identifier': MapToAttributeWithPrefix(prefix='peru_', attribute='rupid'),
    'year': MapToOneValueIfAttributeIsNone(
        attribute='Unnamed: 12', value=3000),
    'month': MapToAttribute('Unnamed: 13'),
    'day': MapToAttribute('Unnamed: 14'),
    'minute': MapToNone(),
    'second': MapToNone(),
    'timeUncertainty': MapToNone(),
    'longitude': MapToAttribute('centroid_lon'),
    'longitudeUncertainty': MapToNone(),
    'latitude': MapToAttribute('centroid_lat'),
    'latitudeUncertainty': MapToNone(),
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
    'type': MapToAttributeAndMapValues('type', {'historic': 'observed'}),
    'probability': MapToOneValue(0.1),
}


def map_to_existing_names(series):
    '''
    Maps the names of the old series to those
    expected by the database.
    '''
    new_series = pd.Series()

    for key in MAPPINGS:
        new_series[key] = MAPPINGS[key](series)
    return new_series


def no_comment_row(x_str):
    '''
    Tests if the row doesn't start as a comment line.
    '''
    return x_str[0] != '#'


def insert_into_sqlite(series, con):
    '''
    Inserts the series in the database.
    '''
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
    '''
    Main function with an argument parser
    to specify the csv file and the sqlite3 database.
    '''
    argparser = argparse.ArgumentParser(
        description='Inserts events to the sqlite database.',
    )
    argparser.add_argument(
        '--csvfile',
        help='Path to the csv file of which the input should be added.',
    )
    argparser.add_argument(
        '--sqlitedb',
        help='Path to the sqlite db to which the data should be added.',
    )

    args = argparser.parse_args()

    do_insert(args.csvfile, args.sqlitedb)


def main_migration():
    '''
    Main function with predefined names for the csv
    file and the sqlite3 database.
    '''
    current_dir = os.path.dirname(os.path.abspath(__file__))
    upper_dir = os.path.dirname(current_dir)

    csvfile = os.path.join(current_dir, 'seismic_catalogue_Peru.csv')
    sqlitedb = os.path.join(upper_dir, 'sqlite3.db')

    do_insert(csvfile, sqlitedb)


def do_insert(csvfile, sqlitedb):
    '''
    Connects to the database and
    inserts all the data of the csvfile.
    '''
    con = sqlite3.connect(sqlitedb)

    data = pd.read_csv(csvfile, skiprows=1)

    for index, row in data.iterrows():
        mapped_row = map_to_existing_names(row)

        insert_into_sqlite(mapped_row, con)

        if index % 1000 == 0:
            print('inserted {}'.format(mapped_row['eventID']))
            con.commit()
    con.commit()


if __name__ == '__main__':
    main_migration()
