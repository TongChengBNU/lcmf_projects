import sys
sys.path.append('/home/chengtong/myGit/lcmf_projects')
sys.path.append('/home/chengtong/myGit/lcmf_projects/lcmf_projects')
import os
from ipdb import set_trace
import logging
logging.basicConfig(filename='RongAn.log', level=logging.WARNING, format="%(asctime)s %(message)s")

import pandas as pd
import database
from ORM_class import rongan_report


logger = logging.getLogger(__name__)


os.system('ls /home/chengtong/myGit/lcmf_projects/Data/rongan_report | grep "融安" > name.txt')
os.system('ls /home/chengtong/myGit/lcmf_projects/Data/rongan_report | grep "综合" >> name.txt')

name_list = []
with open('name.txt', mode='r') as file:
    for line in file:
        name_list.append(line.split("\n")[0])
print("File name.txt is closed? " + str(file.closed) + "\n")

data_path = '/home/chengtong/myGit/lcmf_projects/Data/rongan_report/'
def read_df(filename: str) -> pd.DataFrame:
    print('Filename: ' + filename + '\n')
    return pd.read_excel(data_path + filename)

df_list = []
for name in name_list:
    df_list.append(read_df(name))

standard_column = ['TRADE_DT', 'S_INFO_WINDCODE','S_INFO_WINDNAME','S_INFO_POSITION','S_INFO_COST','S_VAL_MV','S_MV_TO_NAV','S_NEXT_PERIOD','S_NET_ASSET', 'S_COMMENT']

def format_df(df_metadata: pd.DataFrame, show = False) -> pd.DataFrame:
    df_day = df_metadata
    df_standard = df_day.iloc[:, 1:8].copy()
    df_standard.rename(columns={column : standard_column[i] for i, column in enumerate(df_day.columns[1:8])}, inplace=True)
    df_standard.dropna(subset=['TRADE_DT'], inplace=True)

    idiosyncratic_tuples = df_day.iloc[:,-1][ df_day.iloc[:,-1].notnull() ].values
    try:
        s_next_period = idiosyncratic_tuples[0]
        s_net_asset = idiosyncratic_tuples[2]
        s_comment = idiosyncratic_tuples[3:]
        df_standard['S_NEXT_PERIOD'] = s_next_period
        df_standard['S_NET_ASSET'] = s_net_asset
        df_standard['S_COMMENT'] = pd.Series(s_comment)
    except IndexError:
        logging.warning("Error in date: " + str(df_day['日期'].iloc[0])) 

    if show:
        print("-----------------------------------------------\n")
        print(df_standard)
        print("\n")

    return df_standard


df_standard_list = []
for df in df_list:
    df_standard_list.append(format_df(df_metadata=df, show=True))
