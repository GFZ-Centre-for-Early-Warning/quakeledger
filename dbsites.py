#!/usr/bin/env python3

from alchemy_base import Base
from sqlalchemy import Column, Integer, Float

class Site(Base):
    __tablename__ = 'sites'
    id = Column('index', Integer, primary_key=True)
    sid = Column('sid', Integer)
    lon = Column('lon', Float)
    lat = Column('lat', Float)

    def __repr__(self):
        return '<Site(sid=%d, lon=%f, lat=%f)>' % (
                self.sid, self.lon, self.lat)
