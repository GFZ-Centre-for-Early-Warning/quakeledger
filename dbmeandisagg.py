#!/usr/bin/env python3

from alchemy_base import Base
from sqlalchemy import Column, Integer, Float

class MeanDisagg(Base):
    __tablename__ = 'mean_disagg'
    id = Column('index', Integer, primary_key=True)
    sid = Column('sid', Integer)
    poe50y = Column('poe50y', Float)
    lon = Column('Lon', Float)
    lat = Column('Lat', Float)
    mag = Column('Mag', Float)
    poe = Column('poe', Float)

    def __repr__(self):
        return '<MeanDisagg(sid=%d, poe50y=%f, lon=%f, lat=%f, mag=%f, poe=%f)>' % (
                self.sid, self.poe50y, self.lon, self.lat, self.mag)
