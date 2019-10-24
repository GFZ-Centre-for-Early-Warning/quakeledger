#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import sqlalchemy

def main():
    filename = 'peru_events_sqlite3.db'
    engine = sqlalchemy.create_engine(
        'sqlite:///' + filename
    )

    pure_events = pd.read_sql_table('events', engine)
    events = gpd.GeoDataFrame(
        pure_events,
        geometry=gpd.points_from_xy(
            pure_events['longitude'], 
            pure_events['latitude']
        )
    )

    events.to_file('events.json', 'GeoJSON')

if __name__ == '__main__':
    main()
