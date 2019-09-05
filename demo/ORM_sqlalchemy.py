# -*- coding: utf-8 -*-
__author__ = 'TongCheng'
__date__ = '2019/07/10'


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper, relationship, backref
from sqlalchemy import Table, MetaData, Column,  ForeignKey, Boolean
from sqlalchemy import Float, Date, DateTime, Integer, String, Numeric


'''
Database Connection syntax:
mysql_conn_str = "mysql+pymysql://username:password@IP:PORT/database"

如果前缀只用 mysql，会提示缺乏 MySQLdb 的依赖;

'''

db_asset_uri = "mysql+pymysql://root:Mofang123@127.0.0.1/asset_allocation?charset=utf8&use_unicode=1"
engine = create_engine(db_asset_uri, echo=True)
Base = declarative_base()

class jp_huobi_comparison(Base):
    __tablename__ = 'jp_huobi_comparison'
    # __table_args__ = (
    #     ForeignKeyConstrant(),
    #     CheckConstraint(unit_cost , name)
    # )


    jh_date = Column(Date, primary_key=True, nullable=False)
    jh_week_annualized_return_mf = Column(Float, default=None)
    jh_total_rank_mf = Column(String(255), default=None)
    jh_week_annualized_return_qieman = Column(Float, default=None)
    jh_total_rank_qieman = Column(String(255), default=None)
    review = Column(String(255), default=None)


    def __repr__(self):
        return "jp_huobi_comparison(jh_date='{self.jh_date}', " \
                "jh_week_annualized_return_mf='{self.jh_week_annualized_return_mf}', "\
                "jh_total_rank_mf='{self.jh_total_rank_mf}', "\
                "jh_week_annualized_return_qieman='{self.jh_week_annualized_return_qieman}', " \
                "jh_total_rank_qieman='{self.jh_total_rank_qieman}', "\
                "review='{self.review}'".format(self=self)


# Base.metadata.create_all(engine)  # access denied

# sessionmaker is a factory function, it produces a factory(class) 'Session'
Session = sessionmaker(bind=engine)
session = Session()

# 1. Insert
ins1 = jp_huobi_comparison(
    jh_date='2001-01-01',
    jh_week_annualized_return_mf = 0.1,
    jh_total_rank_mf = '1/100',
    jh_week_annualized_return_qieman = 0.2,
    jh_total_rank_qieman = '2/100',
    review = 0
)
ins2 = jp_huobi_comparison(
    jh_date='2001-02-01',
    jh_week_annualized_return_mf = 0.1,
    jh_total_rank_mf = '1/100',
    jh_week_annualized_return_qieman = 0.2,
    jh_total_rank_qieman = '2/100',
    review = 0
)
# session.add(ins1)
# session.add(ins2)
# session.flush()

res = session.query(jp_huobi_comparison.jh_date, jp_huobi_comparison.jh_week_annualized_return_mf, jp_huobi_comparison.jh_total_rank_mf).first()








# # mysql_conn_str = "mysql+pymysql://root:Mofang123@127.0.0.1/asset_allocation?charset=utf8&use_unicode=1"
# mysql_conn_str = "mysql+pymysql://root:chengtong123@localhost:3306/finance"
# engine = create_engine(mysql_conn_str, encoding='utf-8', echo=True)
#
# Base = declarative_base()
#
# # inheritance of class 'Base'
# class mz_markowitz(Base):
#     __tablename__ = "mz_markowitz"
#     globalid = Column(Integer, primary_key=True)
#     mz_name = Column(String)
#     mz_type = Column(Integer)
#     mz_algo = Column(Integer)
#
#
# '''
# metadata = MetaData()
#
# mz_markowitz = Table('mz_markowitz', metadata,
#                      Column('globalid', Integer, primary_key=True),
#                      Column('mz_name', String(200)),
#                      Column('mz_type', Integer),
#                      Column('mz_algo', Integer)
#                  )
#
#
# class item(object):
#     def __init__(self, globalid, mz_name, mz_type, mz_algo):
#         self.id = globalid
#         self.name = mz_name
#         self.type = mz_type
#         self.algorithm = mz_algo
#     def __repr__(self):
#         pass
#
# mapper(item, mz_markowitz)
# initialize database
# def _create_db_table():
#     Base.metadata.create_all(engine)
#
#
#
#
# add record to session.
# objs --> (1) [obj1, obj2]   (2) obj
# def add_records(session, objs):
#     if isinstance(objs, list):
#         session.add_all(objs)
#     else:
#         session.add(objs)
#     session.commit()
#
#
# # extract
# def query_records(session, Cls):
#     return session.query(Cls).all()
#
# '''
#
#
# def create_session(currentEngine):
#     # define class 'Session'
#     Session = sessionmaker(bind=currentEngine)
#     session = Session()
#     return session
#
#
# mySession = create_session(engine)
#
# class macroindex(Base):
#     __tablename__ = 'macroindex'
#
#     # trade_DT = Column(String(20), primary_key=True)
#     # GDP = Column(Float)
#     # CPI = Column(Float)
#     # PPI = Column(Float)
#
#     TRADE_DT = Column(String(20), primary_key=True)
#     GDP_Deflator = Column(Float)
#     CPI_Month = Column(Float)
#     PPI_Industry_Month = Column(Float)
#
#
#
#
# # my_item = mySession.query(mz_markowitz).first()
# # my_item = mySession.query(macroindex).first()
# # my_item = mySession.query(macroindex).one()
# my_item = mySession.query(macroindex).first()
#
# print(my_item.globalid)





