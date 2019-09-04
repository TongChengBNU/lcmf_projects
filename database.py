#coding=utf8

from config import uris
import pymysql
import sqlalchemy

engines = {}


def connection(key: str) -> sqlalchemy.engine:
     if key in engines:
         return engines[key]

     if key in uris:
         uri = uris[key]
         engine = sqlalchemy.create_engine(uri)
         engines[uri] = engine
         return engine


