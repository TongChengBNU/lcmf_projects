from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, Numeric, Float, Date, String 

import pandas as pd

from db import database
from ipdb import set_trace


engine = database.connection('asset')
Session = sessionmaker(bind=engine)
Base = declarative_base()

session = Session()

class financial_product_bank(Base):
    __tablename__  = 'financial_product_bank'

    date = Column(Date(), nullable=False, primary_key=True)
    annual_return = Column(Float, default=None)

class dingkaizhaiji(Base):
    __tablename__ = 'dingkaizhaiji'

    stock_code = Column(Date(), nullable=False, primary_key=True)
    stock_name = Column(String(255), default=None)

df_bank = pd.read_excel('./bankFinancialRate.xls', parse_dates=['date'])
df_dingkai = pd.read_excel('./定开债基_成分股.xlsx')

session = Session()
query = session.query(financial_product_bank)
query.delete()
print("成功删除全部数据;")
#set_trace()
for i in range(len(df_bank)):
   ins = financial_product_bank(
                date = df_bank.iloc[i,0],
                annual_return = df_bank.iloc[i,1]
           )
   session.add(ins)
   session.commit()
   print("成功插入了 %d 条数据;" % (i+1))


#query = session.query(dingkaizhaiji)
#query.delete()
#print("成功删除全部数据;")
#for i in range(len(df_dingkai)):
#    ins = dingkaizhaiji(
#                stock_code = df_dingkai.iloc[i, 0],
#                stock_name = df_dingkai.iloc[i, 1]
#            )
#    session.add(ins)
#    session.commit()
#    print("成功插入了 %d 条数据;" % (i+1))
    
session.close()
