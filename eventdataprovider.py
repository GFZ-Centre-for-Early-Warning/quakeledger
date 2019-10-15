#!/usr/bin/env python3

'''
Classes to load the data from the database
'''

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from eventdb import Site, DeaggregationData, Event

def read_from_sql(query):
    '''
    Utitily function to load all the data from a sql query
    and to fill the None values with nans
    '''
    dataframe = pd.read_sql(query.statement, query.session.bind)
    dataframe.fillna(pd.np.nan, inplace=True)
    return dataframe

class SiteProvider():
    '''
    Class to provide access to the site data
    '''
    def __init__(self, query):
        self.query = query

    def get_nearest(self, lon, lat):
        '''
        Returns the site that is the nearest to the given position
        '''
        sites = read_from_sql(self.query)
        return SiteProvider._find_nearest(sites, lon, lat)

    @staticmethod
    def _find_nearest(sites, lon, lat):
        squared_dists = (sites.lon - lon)**2 + (sites.lat - lat)**2
        nearest_idx = squared_dists.idxmin()
        return sites.iloc[nearest_idx]

class DeaggregationProvider():
    '''
    Class to provide access to the deaggregation data
    '''
    def __init__(self, query):
        self.query = query

    def get_all_for_site_and_poe(self, site, poe):
        '''
        Returns the deaggregation data for one given site
        and a given probability for 50 years
        '''
        return self._get_all_for_sid_and_poe(site.sid, poe)

    def _get_all_for_sid_and_poe(self, sid, poe):
        query = self.query.filter_by(sid=sid)
        query = query.filter_by(poe50y=poe)
        query = query.order_by(DeaggregationData.id)
        return read_from_sql(query)

class EventProvider():
    '''
    Class to provide access to the event data
    '''
    def __init__(self, query):
        self.query = query

    def add_filter_type(self, etype, arg_probability):
        '''
        Adds a filter for the type and the probability
        in the catalog
        Returns nothing
        '''
        if etype in ['expert', 'observed']:
            self.query = self.query.filter_by(type_=etype)
        elif etype in ['stochastic']:
            self.query = self.query.filter_by(type_=etype)
            self.query = self.query.filter(Event.probability > arg_probability)
        elif etype in ['deaggregation']:
            self.query = self.query.filter_by(type_='stochastic')

    def add_filter_magnitude(self, mmin, mmax):
        '''
        Adds a filter for the magnitude
        Returns nothing
        '''
        self.query = self.query.filter(Event.magnitude >= mmin)
        self.query = self.query.filter(Event.magnitude <= mmax)

    def add_filter_spatial(self, lonmin, lonmax, latmin, latmax, zmin, zmax):
        '''
        Adds a spatial filter
        Returns nothing
        '''
        self.query = self.query.filter(Event.longitude >= lonmin)
        self.query = self.query.filter(Event.longitude <= lonmax)
        self.query = self.query.filter(Event.latitude >= latmin)
        self.query = self.query.filter(Event.latitude <= latmax)
        self.query = self.query.filter(Event.depth >= zmin)
        self.query = self.query.filter(Event.depth <= zmax)

    def add_ordering_magnitude_desc(self):
        '''
        Makes sure that the events with the highest magnitude are the first ones
        Returns nothing
        '''
        self.query = self.query.order_by(Event.magnitude.desc())

    def get_results(self):
        '''
        Returns the results as a pandas dataframe
        '''
        return read_from_sql(self.query)

class DataProvider():
    '''
    Overall data provider class
    to give access to the more specific
    data providers
    '''
    def __init__(self, session):
        self.session = session

    def create_provider_for_sites(self):
        '''SiteProvider'''
        return SiteProvider(self.session.query(Site))

    def create_provider_for_mean_deagg(self):
        '''DeaggregationProvider'''
        return DeaggregationProvider(self.session.query(DeaggregationData))

    def create_provider_for_events(self):
        '''EventProvider'''
        return EventProvider(self.session.query(Event))

class Database():
    '''
    Class to store the data for the database connect.
    Should be used with an "with-Statement" to give the
    access to the data provider.
    '''
    def __init__(self, sql_connection_string):
        self.sql_connection_string = sql_connection_string
        self.session = None

    def __enter__(self):
        engine = create_engine(self.sql_connection_string)
        session_maker = sessionmaker(bind=engine)
        self.session = session_maker()
        return DataProvider(self.session)

    def __exit__(self, exception_type, exception_value, trackback):
        self.session.close()

    @classmethod
    def from_local_sql_db(cls, folder, filename):
        '''
        Creates the connection string using a local sqlite database
        Then creates the database instance
        and returns that
        '''
        sql_connection = 'sqlite:///' + folder + filename
        return cls(sql_connection)
