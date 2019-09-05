#coding=utf8


import sys
sys.path.append('shell')
from sqlalchemy import MetaData, Table, select, func, and_
from sqlalchemy import Column, String, Integer, ForeignKey, Text, Date, DateTime, Float
import pandas as pd
import MySQLdb
import logging
from . import database
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dateutil.parser import parse
import time
import numpy as np
from functools import reduce
from ipdb import set_trace

logger = logging.getLogger(__name__)

Base = declarative_base()

class chinamutualfundassetportfolio(Base):

    __tablename__ = 'chinamutualfundassetportfolio'

    OBJECT_ID = Column(Integer, primary_key = True)
    S_INFO_WINDCODE = Column(String)
    F_PRT_ENDDATE = Column(String)
    F_PRT_COVERTBONDTONAV = Column(Float)
    F_PRT_CORPBONDTONAV = Column(Float)
    F_PRT_FINANBONDTONAV = Column(Float)

