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
    This script is mostly the same as the one to
    insert the chile export events (but the old
    one stays with all the peru events, while we
    want here only those that trigger a tsunami).

    So in this case, just drop all the peru events
    and fill the ones from the given file by
    Alireza.
    """

    current_dir = os.path.dirname(os.path.abspath(__file__))
    new_events_db_file = os.path.join(
        current_dir, 'peru_events_sqlite3.db')
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
        # we want to drop all the peru events
        # that are currently in the db
        con.execute("delete from events where eventid like 'peru%'")

    new_events.to_sql('events', old_events_db_engine, if_exists='append', index=False)


if __name__ == '__main__':
    main()
