#!/usr/bin/env python3

import os
import sqlalchemy
import pandas as pd

def drop_columns(table, columns_to_drop):
    cols = []
    set_columns_to_drop = set(columns_to_drop)
    for col in table.columns:
        if col not in set_columns_to_drop:
            cols.append(col)

    return table[cols]


def main():
    """
    This script takes the events table from the
    new database.
    This contains all of the events that can trigger a tsunami for
    chile, so we can delete all the events in the existing database
    that are just chile events.

    The new events must be inserted into the existing database,
    so that we stay with the mean_disagg and site tables.
    """

    current_dir = os.path.dirname(os.path.abspath(__file__))
    new_events_db_file = os.path.join(
        current_dir, 'expert_chile.db')
    old_events_db_file = os.path.join(
        current_dir,
        '..',
        '..',
        'sqlite3.db'
    )

    new_events_db_engine = sqlalchemy.create_engine(
        'sqlite:///' + new_events_db_file
    )

    new_events = pd.read_sql_table('events', new_events_db_engine)
    # because we don't know what the new columns are
    new_events = drop_columns(new_events, ['level_0', 'Unnamed: 0'])
    # and change types to those of the old_events
    # (those are just conversions to strings, so no data will
    # get lost)
    # but there is also a conversion from int64 to float64,
    # but this won't be that hard either
    conversions = {
        'eventID': str,
        'Identifier': str,
        'rake': float
    }
    for column_to_convert in conversions.keys():
        conv_fun = conversions[column_to_convert]
        new_events[column_to_convert] = new_events[column_to_convert].apply(conv_fun)

    old_events_db_engine = sqlalchemy.create_engine(
        'sqlite:///' + old_events_db_file
    )

    with old_events_db_engine.connect() as con:
        # because we have now a very specific list of
        # events for chile
        # we can delete all of the old events for chile
        # but we want to stay with the ones for peru
        con.execute("delete from events where eventid not like 'peru%'")

    new_events.to_sql('events', old_events_db_engine, if_exists='append', index=False)


if __name__ == '__main__':
    main()
