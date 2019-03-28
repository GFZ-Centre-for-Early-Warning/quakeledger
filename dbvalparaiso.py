#!/usr/bin/env python3

from alchemy_base import Base
from sqlalchemy import Column, Integer, Float, String

class Valparaiso(Base):
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

