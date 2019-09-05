import numpy as np
import pandas as pd
import pymysql

# calculate online nav
conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='asset_allocation', charset='utf8')
sql_t = 'select on_online_id, on_date, on_pool_id, on_fund_id, on_fund_code, on_fund_type, on_fund_ratio from on_online_fund where on_online_id = "800000"'
df_online10 = pd.read_sql(sql=sql_t, con=conn, parse_dates=['on_date'], index_col=['on_date']).sort_index()
sql_t = 'select on_online_id, on_date, on_pool_id, on_fund_id, on_fund_code, on_fund_type, on_fund_ratio from on_online_fund where on_online_id = "800001"'
df_online1 = pd.read_sql(sql=sql_t, con=conn, parse_dates=['on_date'], index_col=['on_date']).sort_index()
conn.close()

OnlineAShare = ['11110100', '11110200']
OnlineMShare = ['11120200', '11120201']
OnlineHShare = ['11120500', '11120501']
OnlineShare = OnlineAShare + OnlineMShare + OnlineHShare
OnlineBond = ['11210100', '11210200']
OnlineCash = ['11310100', '11310101']
OnlineCommodity = ['11400100', '11400300']

pool_list = [OnlineAShare, OnlineMShare, OnlineHShare, OnlineShare, OnlineCommodity]  # OnlineBond, OnlineCash
pool_list_str = ['OnlineAShare', 'OnlineMShare', 'OnlineHShare', 'OnlineShare', 'OnlineCommodity']  # 'OnlineBond', 'OnlineCash'
dict_pool = dict()
for i_str in pool_list_str:
    dict_pool[i_str] = pd.DataFrame()

for i_date in df_online10.index.unique():
    for j_num, j_str in enumerate(pool_list_str):
        df_t = df_online10.loc[[i_date]].copy()
        df_t = df_t.loc[df_t.on_pool_id.isin(pool_list[j_num])].copy()
        if df_t.empty:
            df_t = df_online10.loc[[i_date]].copy()
            df_t = df_t.loc[df_t.on_pool_id.isin(OnlineCash)].copy()
        df_t.on_fund_ratio = df_t.on_fund_ratio / df_t.on_fund_ratio.sum()
        dict_pool[j_str] = dict_pool[j_str].append(df_t)


pool_list1 = [OnlineBond, OnlineCash]  # OnlineBond, OnlineCash
pool_list_str1 = ['OnlineBond', 'OnlineCash']  # 'OnlineBond', 'OnlineCash'
for i_str in pool_list_str1:
    dict_pool[i_str] = pd.DataFrame()

for i_date in df_online1.index.unique():
    for j_num, j_str in enumerate(pool_list_str1):
        df_t = df_online1.loc[[i_date]].copy()
        df_t = df_t.loc[df_t.on_pool_id.isin(pool_list1[j_num])].copy()
        if df_t.empty:
            df_t = df_online1.loc[[i_date]].copy()
            df_t = df_t.loc[df_t.on_pool_id.isin(OnlineCash)].copy()
        if df_t.empty:
            continue
        df_t.on_fund_ratio = df_t.on_fund_ratio / df_t.on_fund_ratio.sum()
        dict_pool[j_str] = dict_pool[j_str].append(df_t)

pool_list_str_all = pool_list_str + pool_list_str1
for i_str in pool_list_str_all:
    df_t = dict_pool[i_str].reset_index().copy()
    df_t.on_fund_ratio = df_t.on_fund_ratio.round(4)
    dict_pool[i_str] = df_t


conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='asset_allocation', charset='utf8')
x = conn.cursor()
globalid_list = ['PO.'+i for i in pool_list_str_all]
for i_num, i_str in enumerate(globalid_list):
    df_t = dict_pool[pool_list_str_all[i_num]].copy()
    df_t = df_t.loc[df_t.on_date >= pd.Timestamp(2019, 3, 29)]
    print(df_t)
    for j_num in range(df_t.shape[0]):
        try:
            excuse_ = "INSERT INTO `ra_portfolio_pos` (`ra_portfolio_id`, `ra_date`, `ra_pool_id`, `ra_fund_id`, `ra_fund_code`, `ra_fund_type`, `ra_fund_ratio`, `created_at`, `updated_at`) VALUES ('%s', '%s', '%s', '%s','%s', '%s',' %s', '2019-05-30 17:06:39', '2019-05-30 17:06:39');" % (i_str, df_t[['on_date']].iloc[j_num, 0], df_t[['on_pool_id']].iloc[j_num, 0], df_t[['on_fund_id']].iloc[j_num, 0], df_t[['on_fund_code']].iloc[j_num, 0], df_t[['on_fund_type']].iloc[j_num, 0], df_t[['on_fund_ratio']].iloc[j_num, 0])

            x.execute(excuse_)
            conn.commit()
        except:
            print('error')
            conn.rollback()
conn.close()

# 单独计算债券
########################################################################################################################
import numpy as np
import pandas as pd
import pymysql

conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='asset_allocation', charset='utf8')
ra_pool1 = ['11210100', '11210200']
sql_t = 'select * from ra_pool_fund'
df_pool_fund = pd.read_sql(sql=sql_t, con=conn, parse_dates=['ra_date'])

df_ra_pool1 = df_pool_fund.loc[df_pool_fund.ra_pool.isin(ra_pool1)].set_index('ra_date').sort_index()

dict_pool1 = dict()
dict_pool1['11210100'] = df_pool_fund.loc[df_pool_fund.ra_pool == '11210100'].set_index('ra_date').sort_index()
dict_pool1['11210200'] = df_pool_fund.loc[df_pool_fund.ra_pool == '11210200'].set_index('ra_date').sort_index()

dict_pool_change1 = dict()
dict_pool_change1['11210100'] = pd.DataFrame()
dict_pool_change1['11210200'] = pd.DataFrame()
for i_date in df_ra_pool1.index.unique():
    for j_pool in dict_pool1.keys():
        df_t = dict_pool1[j_pool].copy()
        df_t = df_t.loc[:i_date]
        select_date = df_t.index[-1]
        df_t = df_t.loc[[select_date]]
        df_t['pos'] = 0.5/df_t.shape[0]
        df_t.reset_index(inplace=True)
        df_t.ra_date = i_date
        df_t.set_index('ra_date', inplace=True)
        dict_pool_change1[j_pool] = dict_pool_change1[j_pool].append(df_t)

df_pos = pd.DataFrame()
for i_pool in dict_pool_change1.keys():
    df_pos = df_pos.append(dict_pool_change1[i_pool])
df_pos.sort_index(inplace=True)

conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='asset_allocation', charset='utf8')
x = conn.cursor()
i_str = 'PO.OnlineBond'
df_t = df_pos.reset_index()
df_t.pos = df_t.pos.round(4)
for j_num in range(df_t.shape[0]):
    try:
        excuse_ = "INSERT INTO `ra_portfolio_pos` (`ra_portfolio_id`, `ra_date`, `ra_pool_id`, `ra_fund_id`, `ra_fund_code`, `ra_fund_type`, `ra_fund_ratio`, `created_at`, `updated_at`) VALUES ('%s', '%s', '%s', '%s','%s', '%s',' %s', '2019-05-30 17:06:39', '2019-05-30 17:06:39');" % (i_str, df_t[['ra_date']].iloc[j_num, 0], df_t[['ra_pool']].iloc[j_num, 0], df_t[['ra_fund_id']].iloc[j_num, 0], df_t[['ra_fund_code']].iloc[j_num, 0], df_t[['ra_fund_type']].iloc[j_num, 0], df_t[['pos']].iloc[j_num, 0])

        x.execute(excuse_)
        conn.commit()
    except:
        print('error')
        conn.rollback()
conn.close()

