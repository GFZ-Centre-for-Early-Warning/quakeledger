#!/usr/bin/env python3

"""
ORM classes to access the data in the database
"""

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DeaggregationData(Base):
    """Class for accessing the data for the mean disaggregation"""

    __tablename__ = "mean_disagg"
    id = Column("index", Integer, primary_key=True)
    sid = Column("sid", Integer)
    poe50y = Column("poe50y", Float)
    lon = Column("Lon", Float)
    lat = Column("Lat", Float)
    mag = Column("Mag", Float)
    poe = Column("poe", Float)

    def __repr__(self):
        return (
            "<DisaggData(sid=%d, poe50y=%f, lon=%f, lat=%f, mag=%f, poe=%f)>"
            % (self.sid, self.poe50y, self.lon, self.lat, self.mag, self.poe)
        )


class Site(Base):
    """Class to access the data of the sites"""

    __tablename__ = "sites"
    id = Column("index", Integer, primary_key=True)
    sid = Column("sid", Integer)
    lon = Column("lon", Float)
    lat = Column("lat", Float)

    def __repr__(self):
        return "<Site(sid=%d, lon=%f, lat=%f)>" % (
            self.sid,
            self.lon,
            self.lat,
        )


class Event(Base):
    """Class to access to data of the earth quake events"""

    __tablename__ = "events"
    id = Column("index", Integer, primary_key=True)
    eventID = Column("eventID", String)
    identifier = Column("Identifier", String)
    year = Column("year", Integer)
    month = Column("month", Float)
    day = Column("day", Float)
    hour = Column("hour", Float)
    minute = Column("minute", Float)
    second = Column("second", Float)
    timeUncertainty = Column("timeUncertainty", Float)
    longitude = Column("longitude", Float)
    longitudeUncertainty = Column("longitudeUncertainty", Float)
    latitude = Column("latitude", Float)
    latitudeUncertainty = Column("latitudeUncertainty", Float)
    horizontalUncertainty = Column("horizontalUncertainty", Float)
    minHorizontalUncertainty = Column("minHorizontalUncertainty", Float)
    maxHorizontalUncertainty = Column("maxHorizontalUncertainty", Float)
    azimuthMaxHorizontalUncertainty = Column(
        "azimuthMaxHorizontalUncertainty", Float
    )
    depth = Column("depth", Float)
    depthUncertainty = Column("depthUncertainty", Float)
    magnitude = Column("magnitude", Float)
    magnitudeUncertainty = Column("magnitudeUncertainty", Float)
    rake = Column("rake", Float)
    rakeUncertainty = Column("rakeUncertainty", Float)
    dip = Column("dip", Float)
    dipUncertainty = Column("dipUncertainty", Float)
    strike = Column("strike", Float)
    strikeUncertainty = Column("strikeUncertainty", Float)
    type_ = Column("type", String)
    probability = Column("probability", Float)
