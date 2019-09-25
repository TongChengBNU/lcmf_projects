#coding=utf8


from sqlalchemy import MetaData, Table, select, func 
from sqlalchemy import Column, String, Integer, ForeignKey, Text, Date, DateTime, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from ipdb import set_trace

Base = declarative_base()

class rongan_report(Base):

    __tablename__ = 'rongan_report'

    TRADE_DT = Column(Date, default=None, primary_key=True)
    S_INFO_WINDCODE = Column(String, default=None, primary_key=True)
    S_INFO_WINDNAME = Column(String, default=None)
    S_INFO_POSITION = Column(Float, default=None)
    S_INFO_COST = Column(Float, default=None)
    S_VAL_MV = Column(Float, default=None)
    S_MV_TO_NAV = Column(Float, default=None)
    S_NEXT_PERIOD = Column(Float, default=None)
    S_NET_ASSET = Column(Float, default=None)
    S_COMMENT = Column(String, default=None)

