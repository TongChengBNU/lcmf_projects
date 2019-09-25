import sys
sys.path.append('/home/chengtong/myGit/lcmf_projects')
sys.path.append('/home/chengtong/myGit/lcmf_projects/lcmf_projects')
import os
from ipdb import set_trace
import logging
logging.basicConfig(filename='RongAn.log', level=logging.WARNING, format="%(asctime)s %(message)s")

import pandas as pd
import numpy as np
import database
from ORM_class import rongan_report
from sqlalchemy.orm import sessionmaker


logger = logging.getLogger(__name__)


os.system('ls /home/chengtong/myGit/lcmf_projects/Data/rongan_report | grep "融安1号-成交反馈" > name.txt')
os.system('ls /home/chengtong/myGit/lcmf_projects/Data/rongan_report | grep "综合" >> name.txt')

name_list = []
with open('name.txt', mode='r') as file:
    for line in file:
        name_list.append(line.split("\n")[0])
print("File name.txt is closed? " + str(file.closed) + "\n")

data_path = '/home/chengtong/myGit/lcmf_projects/Data/rongan_report/'
def read_df(filename: str) -> pd.DataFrame:
    print('Filename: ' + filename + '\n')
    return pd.read_excel(io = data_path + filename, converters = {2:str})

df_dict = dict()
for name in name_list:
    df_dict[name] = read_df(filename = name)

standard_column = ['TRADE_DT', 'S_INFO_WINDCODE','S_INFO_WINDNAME','S_INFO_POSITION','S_INFO_COST','S_VAL_MV','S_MV_TO_NAV','S_NEXT_PERIOD','S_NET_ASSET', 'S_COMMENT']

def format_df(df_metadata: pd.DataFrame, show = False) -> pd.DataFrame:
    df_day = df_metadata
    df_standard = df_day.iloc[:, 1:8].copy()
    df_standard.rename(columns={column : standard_column[i] for i, column in enumerate(df_day.columns[1:8])}, inplace=True)
    df_standard.dropna(subset=['TRADE_DT'], inplace=True)

    #idiosyncratic_tuples = df_day.iloc[:,-1][ df_day.iloc[:,-1].notnull() ].values
    idiosyncratic_tuples = df_day.iloc[:,-1].values
    try:
        s_next_period = idiosyncratic_tuples[0]
        s_net_asset = idiosyncratic_tuples[2]
        s_comment = idiosyncratic_tuples[3:]
        df_standard['S_NEXT_PERIOD'] = s_next_period
        df_standard['S_NET_ASSET'] = s_net_asset
        df_standard['S_COMMENT'] = pd.Series(s_comment)
    except IndexError:
        logging.warning("Error in date: " + str(df_day.iloc[0, 1])) 

    df_standard.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
    df_standard.fillna(value=None, inplace=True)
    if show:
        print("-----------------------------------------------\n")
        print(df_standard)
        print("\n")

    return df_standard


def format_df_name(name: str, show = False) -> pd.DataFrame:
    df_day = df_dict[name]
    df_standard = df_day.iloc[:, 1:8].copy()
    df_standard.rename(columns={column : standard_column[i] for i, column in enumerate(df_day.columns[1:8])}, inplace=True)
    df_standard.dropna(subset=['TRADE_DT'], inplace=True)

    #idiosyncratic_tuples = df_day.iloc[:,-1][ df_day.iloc[:,-1].notnull() ].values
    idiosyncratic_tuples = df_day.iloc[:,-1].values
    try:
        s_next_period = idiosyncratic_tuples[0]
        s_net_asset = idiosyncratic_tuples[2]
        s_comment = idiosyncratic_tuples[3:]
        df_standard['S_NEXT_PERIOD'] = s_next_period
        df_standard['S_NET_ASSET'] = s_net_asset
        df_standard['S_COMMENT'] = pd.Series(s_comment)
    except IndexError:
        logging.warning("Error in date: " + str(df_day.iloc[0, 1])) 

    df_standard.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
    df_standard.fillna(value=None, inplace=True)
    if show:
        print("-----------------------------------------------\n")
        print(df_standard)
        print("\n")

    return df_standard


df_standard_dict = dict()
for name in df_dict:
    df_standard_dict[name] = format_df_name(name=name, show=False)


def query(num):
    name = name_list[num]
    print("File name: " + name + "\n")
    print(df_standard_dict[name])
    return


engine = database.connection(key = 'asset_allocation')
Session = sessionmaker(bind = engine)


session = Session()
query = session.query(rongan_report)
query.delete()
session.commit()
print("成功清除数据;")
session.close()


def insert(df_metadata: pd.DataFrame, quiet = True) -> bool:
    session = Session()
    for i, series in df_metadata.iterrows():
        tmp = tuple(map(lambda x: x if not str(x) == 'nan' else None,  series.values ))
        ins = rongan_report(
            TRADE_DT = tmp[0], 
            S_INFO_WINDCODE = tmp[1], 
            S_INFO_WINDNAME = tmp[2],
            S_INFO_POSITION = tmp[3],
            S_INFO_COST = tmp[4],
            S_VAL_MV = tmp[5],
            S_MV_TO_NAV = tmp[6],
            S_NEXT_PERIOD = tmp[7],
            S_NET_ASSET = tmp[8],
            S_COMMENT = tmp[9] 
        )
        session.add(ins)
        session.commit()
    if not quiet:
        print("表插入完成，共 %d 条数据;" % len(df_metadata))
    return True

def insert_name(name: str, quiet = True) -> bool:
    session = Session()
    for i, series in df_metadata.iterrows():
        tmp = tuple(map(lambda x: x if not str(x) == 'nan' else None,  series.values ))
        ins = rongan_report(
            TRADE_DT = tmp[0], 
            S_INFO_WINDCODE = tmp[1], 
            S_INFO_WINDNAME = tmp[2],
            S_INFO_POSITION = tmp[3],
            S_INFO_COST = tmp[4],
            S_VAL_MV = tmp[5],
            S_MV_TO_NAV = tmp[6],
            S_NEXT_PERIOD = tmp[7],
            S_NET_ASSET = tmp[8],
            S_COMMENT = tmp[9] 
        )
        session.add(ins)
        session.commit()
    if not quiet:
        print("%s 表插入完成，共 %d 条数据;" % (name, len(df_metadata)) )
        
    return True

if __name__ == '__main__':
    for name in name_list:
        try:
            ret = insert(df_standard_dict[name], quiet=False)
            if not ret:
                logging.warning("Error in %s;" % name)
        except:
            logging.warning("Error in %s;" % name)


















