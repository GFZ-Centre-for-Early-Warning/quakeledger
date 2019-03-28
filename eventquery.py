#!/usr/bin/env/python

import os
import sys

import lxml.etree as le
import numpy as np
import pandas as pd

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from eventwriter import QuakeMlWriter

Base = declarative_base()

### basic functions #####################################################################

def read_from_sql(query):
    df = pd.read_sql(query.statement, query.session.bind)
    df.fillna(np.nan, inplace=True)
    return df


### ORM data classes ####################################################################

class DeaggregationData(Base):
    __tablename__ = 'mean_disagg'
    id = Column('index', Integer, primary_key=True)
    sid = Column('sid', Integer)
    poe50y = Column('poe50y', Float)
    lon = Column('Lon', Float)
    lat = Column('Lat', Float)
    mag = Column('Mag', Float)
    poe = Column('poe', Float)

    def __repr__(self):
        return '<DisaggData(sid=%d, poe50y=%f, lon=%f, lat=%f, mag=%f, poe=%f)>' % (
                self.sid, self.poe50y, self.lon, self.lat, self.mag)

class Site(Base):
    __tablename__ = 'sites'
    id = Column('index', Integer, primary_key=True)
    sid = Column('sid', Integer)
    lon = Column('lon', Float)
    lat = Column('lat', Float)

    def __repr__(self):
        return '<Site(sid=%d, lon=%f, lat=%f)>' % (
                self.sid, self.lon, self.lat)

class Event(Base):
    __tablename__ = 'valparaiso'
    id = Column('index', Integer, primary_key=True)
    eventID = Column('eventID', String)
    identifier = Column('Identifier', String)
    year = Column('year', Integer)
    month = Column('month', Float)
    day = Column('day', Float)
    hour = Column('hour', Float)
    minute = Column('minute', Float)
    second = Column('second', Float)
    timeUncertainty = Column('timeUncertainty', Float)
    longitude = Column('longitude', Float)
    longitudeUncertainty = Column('longitudeUncertainty', Float)
    latitude = Column('latitude', Float)
    latitudeUncertainty = Column('latitudeUncertainty', Float)
    horizontalUncertainty = Column('horizontalUncertainty', Float)
    minHorizontalUncertainty = Column('minHorizontalUncertainty', Float)
    maxHorizontalUncertainty = Column('maxHorizontalUncertainty', Float)
    azimuthMaxHorizontalUncertainty = Column('azimuthMaxHorizontalUncertainty', Float)
    depth = Column('depth', Float)
    depthUncertainty = Column('depthUncertainty', Float)
    magnitude = Column('magnitude', Float)
    magnitudeUncertainty = Column('magnitudeUncertainty', Float)
    rake = Column('rake', Float)
    rakeUncertainty = Column('rakeUncertainty', Float)
    dip = Column('dip', Float)
    dipUncertainty = Column('dipUncertainty', Float)
    strike = Column('strike', Float)
    strikeUncertainty = Column('strikeUncertainty', Float)
    type_ = Column('type', String)
    probability = Column('probability', Float)

### classes for deaggretion #############################################################

class DeaggregationResult():
    def __init__(self, data):
        self.data = data

    def add_filter_spatial(self, lonmin, lonmax, latmin, latmax, zmin, zmax):
        self.data = self.data[
                (self.data.longitude >= lonmin) &
                (self.data.longitude <= lonmax) &
                (self.data.latitude >= latmin) &
                (self.data.latitude <= latmax) &
                (self.data.depth >= zmin) &
                (self.data.depth <= zmax)
        ]

    def add_filter_magnitude(self, mmin, mmax):
        self.data = self.data[
                (self.data.magnitude >= mmin) &
                (self.data.magnitude <= mmax)
        ]

    def add_ordering_magnitude_desc(self):
        self.data = self.data.sort_values('magnitude', ascending=False)

    def get_result(self):
        return self.data

    def get_results(self):
        return self.data

class DeaggregationMatcher():
    def __init__(self, deagg_data, p_lon, p_lat, p_mag):
        self.deagg_data = deagg_data
        self.p_lon = p_lon
        self.p_lat = p_lat
        self.p_mag = p_mag
        self.deagg_data_p_gr_0 = self.deagg_data[self.deagg_data.poe > 0]

    def match_deaggregation(self, event_data):
        bins = self._binning_xyz(event_data[['longitude', 'latitude', 'magnitude']])
        idx, poe = self._choose_random_events(bins, seed=42)
        matches = event_data.loc[idx].copy()
        matches['probability'] = poe
        return DeaggregationResult(matches)

    def _choose_random_events(self, bins, seed=42):
        idxs = []
        poe = []
        #go through bins of deaggregation
        for i in range(len(self.deagg_data_p_gr_0)):
            seed += i
            row = self.deagg_data_p_gr_0.iloc[i]
            #get events
            matches = self._find_matches(bins, row)
            #append single random sampled idx
            n_matches = len(matches)
            if n_matches > 0:
                np.random.seed(seed)
                idx = np.random.randint(0, n_matches, 1)[0]
                idxs.append(matches.iloc[idx].name)
                poe.append(row.poe)

        return idxs, poe

    def _find_matches(self, bins, row):
        return bins[
                (abs(bins.longitude - row.Lon) < 10**-5) &
                (abs(bins.latitude  - row.Lat) < 10**-5) &
                (abs(bins.magnitude - row.Mag) < 10**-5)
        ]



    def _binning_xyz(self, data):
        px = self.p_lon
        py = self.p_lat
        pz = self.p_mag
        
        xyz = data.copy()
        cols = xyz.columns

        #rounds to bin precision
        xyz[cols[0]] = xyz[cols[0]] / px
        xyz[cols[1]] = xyz[cols[1]] / py
        xyz[cols[2]] = xyz[cols[2]] / pz
        xyz = xyz.round()
        xyz[cols[0]] = xyz[cols[0]] * px
        xyz[cols[1]] = xyz[cols[1]] * py
        xyz[cols[2]] = xyz[cols[2]] * pz
        return xyz

class DeaggregationAnalyzer():
    def __init__(self, data):
        self.data = data

    def _get_precision_lon(self):
        return round(min(np.diff(self.data.Lon.unique())), 5)

    def _get_precision_lat(self):
        return round(min(np.diff(self.data.Lat.unique())), 5)

    def _get_precision_mag(self):
        return round(min(np.diff(self.data.Mag.unique())), 5)

    def get_precisions_lon_lat_mag(self):
        return (
                self._get_precision_lon(),
                self._get_precision_lat(),
                self._get_precision_mag()
        )


### classes for providing access to the data ############################################

class SiteProvider():
    def __init__(self, query):
        self.query = query

    def get_nearest(self, lon, lat):
        sites = read_from_sql(self.query)
        return self._find_nearest(sites, lon, lat)

    def _find_nearest(self, sites, lon, lat):
        squared_dists = (sites.lon - lon)**2 + (sites.lat - lat)**2
        nearest_idx = squared_dists.idxmin()
        return sites.iloc[nearest_idx]

class DeaggregationProvider():
    def __init__(self, query):
        self.query = query

    def get_all_for_site_and_poe(self, site, poe):
        return self._get_all_for_sid_and_poe(site.sid, poe)

    def _get_all_for_sid_and_poe(self, sid, poe):
        query = self.query.filter_by(sid=sid)
        query = query.filter_by(poe50y = poe)
        query = query.order_by(DeaggregationData.id)
        return read_from_sql(query)

class EventProvider():
    def __init__(self, query):
        self.query = query

    def add_filter_type(self, etype, probability):
        if etype in ['expert', 'observed']:
            self.query = self.query.filter_by(type_ = etype)
        elif etype in ['stochastic']:
            self.query = self.query.filter_by(type_ = etype)
            self.query = self.query.filter_by(probability > probability)
        elif etype in ['deaggregation']:
            self.query = self.query.filter_by(type_ = 'stochastic')

    def add_filter_magnitude(self, mmin, mmax):
        self.query = self.query.filter(Event.magnitude >= mmin)
        self.query = self.query.filter(Event.magnitude <= mmax)

    def add_filter_spatial(self, lonmin, lonmax, latmin, latmax, zmin, zmax):
        self.query = self.query.filter(Event.longitude >= lonmin)
        self.query = self.query.filter(Event.longitude <= lonmax)
        self.query = self.query.filter(Event.latitude >= latmin)
        self.query = self.query.filter(Event.latitude <= latmax)
        self.query = self.query.filter(Event.depth >= zmin)
        self.query = self.query.filter(Event.depth <= zmax)

    def add_ordering_magnitude_desc(self):
        self.query = self.query.order_by(Event.magnitude.desc())

    def get_results(self):
        return read_from_sql(self.query)

class DataProvider():
    def __init__(self, session):
        self.session = session

    def create_provider_for_sites(self):
        return SiteProvider(self.session.query(Site))

    def create_provider_for_mean_deagg(self):
        return DeaggregationProvider(self.session.query(DeaggregationData))

    def create_provider_for_earthquakes(self):
        return EventProvider(self.session.query(Event))

class Database():
    def __init__(self, sql_connection_string):
        self.sql_connection_string = sql_connection_string

    def __enter__(self):
        engine = create_engine(self.sql_connection_string)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        return DataProvider(self.session)

    def __exit__(self, exception_type, exception_value, trackback):
        self.session.close()

    @classmethod
    def from_local_sql_db(cls, filename):
        filepath = os.path.dirname(__file__)
        sql_connection = 'sqlite:///' + filepath + filename
        return cls(sql_connection)

### main ################################################################################

class Main():
    def __init__(self):
        self.db = Database.from_local_sql_db('sqlite3.db')
        self.lonmin = float(sys.argv[1])
        self.lonmax = float(sys.argv[2])
        self.latmin = float(sys.argv[3])
        self.latmax = float(sys.argv[4])
        self.mmin = float(sys.argv[5])
        self.mmax = float(sys.argv[6])
        self.zmin = float(sys.argv[7])
        self.zmax = float(sys.argv[8])
        self.p = float(sys.argv[9])
        self.etype = sys.argv[10]
        self.tlon = float(sys.argv[11])
        self.tlat = float(sys.argv[12])
        self.num_events = -1

        self.precision_lon = 0
        self.precision_lat = 0
        self.precision_mag = 0

        self.optional_deaggregation_data = None

        self.selected = None

        self.filename_output = 'test.xml'

    def _check_longitude(self):
        if self.lonmin > 180:
            self.lonmin = self._convert_360(self.lonmin)
        if self.lonmax > 180:
            self.lonmax = self._convert_360(self.lonmax)

    def _convert_360(self, lon):
        '''
        convert a longitude specified with 180+
        '''
        return lon-360

    def _needs_deaggreation(self):
        return self.etype == 'deaggregation'

    def _prepare_deaggreation(self, data_provider):
        site = data_provider.create_provider_for_sites().get_nearest(self.tlon, self.tlat)
        self.optional_deaggregation_data = data_provider.create_provider_for_mean_deagg().get_all_for_site_and_poe(site, self.p)
        deagg_analyzer = DeaggregationAnalyzer(self.optional_deaggregation_data)
        p_lon, p_lat, p_mag = deagg_analyzer.get_precisions_lon_lat_mag()
        self.precision_lon, self.precision_lat, self.precision_mag = p_lon, p_lat, p_mag

    def _deaggregate(self):
        matcher = DeaggregationMatcher(
                self.optional_deaggregation_data, 
                self.precision_lon, 
                self.precision_lat, 
                self.precision_mag)
        deaggregation_result = matcher.match_deaggregation(self.selected)
        # another filter
        # because the deaggregation has needed a bigger extend of data
        deaggregation_result.add_filter_spatial(
                self.lonmin, self.lonmax, self.latmin, self.latmax, self.zmin, self.zmax)
        deaggregation_result.add_filter_magnitude(self.mmin, self.mmax)
        deaggregation_result.add_ordering_magnitude_desc()
        
        return deaggregation_result.get_result()

    def _write_file(self):

        writer = QuakeMlWriter(self.filename_output)
        writer.write(self.selected)

    def _filter_num_events(self):
        if self.num_events > 0:
            self.selected = self.selected.iloc[0:self.num_events]

    def run(self):
        self._check_longitude()

        with self.db as data_provider:

            event_provider = data_provider.create_provider_for_earthquakes()
            event_provider.add_filter_type(self.etype, self.p)

            if self._needs_deaggreation():
                self._prepare_deaggreation(data_provider)
            
            event_provider.add_filter_spatial(
                    self.lonmin - self.precision_lon * 2, 
                    self.lonmax + self.precision_lon * 2, 
                    self.latmin - self.precision_lat * 2, 
                    self.latmax + self.precision_lat * 2, 
                    self.zmin, 
                    self.zmax)
            event_provider.add_filter_magnitude(
                    self.mmin - self.precision_mag * 2, 
                    self.mmax + self.precision_mag * 2)
            event_provider.add_ordering_magnitude_desc()
            
            self.selected = event_provider.get_results()

            if self._needs_deaggreation():
                self.selected = self._deaggregate()

            self._filter_num_events()
            self._write_file()

if __name__ =='__main__':
    Main().run()
