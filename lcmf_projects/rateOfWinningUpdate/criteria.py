import numpy as np
import pandas as pd
import pymysql
begin_date = pd.Timestamp(2016, 8, 12)
end_date = pd.Timestamp(2019, 8, 8)

conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='asset_allocation', charset='utf8')
sql_t = 'select ra_date, ra_portfolio_id, ra_nav from ra_portfolio_nav where ra_type = 8'
df_Risk10 = pd.read_sql(sql=sql_t, con=conn, parse_dates=['ra_date'])
ids = ['PO.OnlineAShare', 'PO.OnlineMShare', 'PO.OnlineHShare', 'PO.OnlineShare', 'PO.OnlineBond', 'PO.OnlineCommodity']
df_Risk10 = df_Risk10.set_index(['ra_date', 'ra_portfolio_id']).unstack()['ra_nav'].reindex(ids, axis=1)
# df_Risk10_ = pd.read_csv(r'C:\Users\ASUS\Desktop\on_online_nav_risk.csv', parse_dates=['on_date'], index_col=['on_date']).rename(columns={'on_nav': 'lcmf_risk10'})
# df_Risk10 = pd.merge(df_Risk10, df_Risk10_, how='inner', left_index=True, right_index=True)
conn.close()

df_CashPortfolio = pd.read_csv(r'C:\Users\ASUS\Desktop\statistic\ra_pool_cash.csv', parse_dates=['ra_date'], index_col=['ra_date'])
# ra_pool_nav 11310102
df_Steady = pd.read_csv(r'C:\Users\ASUS\Desktop\statistic\ra_portfolio_nav.csv', parse_dates=['ra_date'], index_col=['ra_date'])[['ra_nav']]
# ra_portfolio_nav PO.CB0040
conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='mofang', charset='utf8')
sql_t = 'select ra_fund_id, ra_date, ra_nav_adjusted from ra_fund_nav'
ra_fund_nav = pd.read_sql(sql=sql_t, con=conn, parse_dates=['ra_date'])
ra_fund_nav.ra_fund_id = ra_fund_nav.ra_fund_id.astype(str)
ra_fund_nav = ra_fund_nav.drop_duplicates(subset=['ra_date', 'ra_fund_id']).set_index(['ra_date', 'ra_fund_id']).unstack()['ra_nav_adjusted'].sort_index()

sql_t = 'select * from trade_dates'
df_t = pd.read_sql(con=conn, sql=sql_t, parse_dates=['td_date'], index_col=['td_date'])
trade_weeks = df_t.loc[((df_t.td_type & 0X02)>0) & (df_t.index > begin_date)].index.sort_values()
# notice
trade_weeks = list(trade_weeks)
trade_weeks[-1] = pd.Timestamp(2019, 8, 22)
conn.close()

conn = pymysql.connect(host='192.168.88.18', user='readonly', passwd='readonly123', database='mofang_api', charset='utf8')
sql_t = 'select * from yinhe_type'
yinhe_type = pd.read_sql(sql=sql_t, con=conn)
yinhe_type.yt_fund_id = yinhe_type.yt_fund_id.astype(str)
conn.close()

bank_nav = pd.read_excel(r'C:\Users\ASUS\Desktop\statistic\银行理财产品预期年收益率人民币.xls', parse_dates=['date'])
bank_nav.set_index('date', inplace=True)
bank_nav['mean'] = bank_nav.mean(axis=1)

pool_str = ['智能组合', '货币基金池', '债券基金池', 'a股基金池', '美股基金池', '港股基金池']
dict_PoolCode = dict()
# t = ['中短期标准债券型基金', '中短期标准债券型基金(B/C类)', '长期标准债券型基金(A类)', '长期标准债券型基金(B/C类)', '指数债券型基金(A类)', '指数债券型基金(B/C类)']
t = ['中短期标准债券型基金', '中短期标准债券型基金(B/C类)', '长期标准债券型基金(B/C类)', '指数债券型基金(B/C类)']
BondFund_t = yinhe_type.loc[yinhe_type.yt_l3_name.isin(t)].copy()
dingkai = pd.read_excel(r'C:\Users\ASUS\Desktop\statistic\定开债基 成分股.xlsx')
dingkai['code'] = dingkai['证券代码'].apply(lambda x: x[:-3])
BondFund_t = BondFund_t.loc[~BondFund_t.yt_fund_code.isin(dingkai.code.unique())]

df_ttt = yinhe_type.loc[yinhe_type.yt_l1_name.isin(['混合基金'])].copy()
codes_t = list(yinhe_type.loc[yinhe_type.yt_l1_name.isin(['股票基金']), 'yt_fund_id'].unique()) + list(df_ttt.loc[df_ttt.yt_l2_name.isin(['偏股型基金', '股债平衡型基金']), 'yt_fund_id'].unique())
dict_PoolCode['智能组合'] = set(codes_t)

dict_PoolCode['货币组合'] = yinhe_type.loc[yinhe_type.yt_l1_name =='货币市场基金', 'yt_fund_id'].unique()
dict_PoolCode['债券基金池'] = BondFund_t.yt_fund_id.unique()
dict_PoolCode['a股基金池'] = yinhe_type.loc[yinhe_type.yt_l1_name =='股票基金', 'yt_fund_id'].unique()
dict_PoolCode['美股基金池'] = yinhe_type.loc[yinhe_type.yt_l2_name =='QDII股票基金', 'yt_fund_id'].unique()
dict_PoolCode['港股基金池'] = yinhe_type.loc[yinhe_type.yt_l2_name =='QDII股票基金', 'yt_fund_id'].unique()

# 月收益比较
df_CompareMonth = df_Risk10[['PO.OnlineAShare', 'PO.OnlineMShare', 'PO.OnlineHShare', 'PO.OnlineBond']].copy()
df_CompareMonth.rename(columns={'PO.OnlineAShare': 'a股基金池', 'PO.OnlineMShare': '美股基金池', 'PO.OnlineHShare': '港股基金池', 'PO.OnlineBond': '债券基金池'}, inplace=True)
df_MonthRank = pd.DataFrame()
frequency = 4  # one month = four week
for i_pool in df_CompareMonth.columns:
    df_FundNav_t = ra_fund_nav.reindex(dict_PoolCode[i_pool], axis=1).copy()
    df_FundNav_t = pd.merge(df_FundNav_t, df_CompareMonth[[i_pool]], how='inner', left_index=True, right_index=True).reindex(trade_weeks).sort_index()
    for last_trade_date, trade_date in zip(df_FundNav_t.index[:-frequency], df_FundNav_t.index[frequency:]):
        ser_FundReturn_t = df_FundNav_t.loc[[last_trade_date, trade_date]].pct_change().iloc[-1].dropna()
        ser_FundRank_t = ser_FundReturn_t.rank(ascending=False)
        t = ser_FundRank_t[i_pool] / len(ser_FundRank_t)
        if t < 0.5:
            df_MonthRank.at[trade_date, i_pool] = 1
        else:
            df_MonthRank.at[trade_date, i_pool] = 0
df_MonthRank_Rolling = df_MonthRank.rolling(52).apply(lambda x: np.sum(x)/52, 'raw=True')

# 周收益比较
# df_CompareWeek = pd.merge(df_Risk10[['PO.OnlineCash']], df_CashPortfolio, how='inner', left_index=True, right_index=True)
# df_CompareWeek = df_CompareWeek.rename(columns={'PO.OnlineCash': '货币基金池', 'ra_nav': '货币组合'}).reindex(trade_weeks)
df_CompareWeek = df_CashPortfolio.rename(columns={'ra_nav': '货币组合'}).reindex(trade_weeks)
df_WeekRank = pd.DataFrame()
df_FundNav_t = ra_fund_nav.reindex(dict_PoolCode['货币组合'], axis=1).copy()
df_FundNav_t = pd.merge(df_FundNav_t, df_CompareWeek, how='inner', left_index=True, right_index=True).dropna(subset=['货币组合']).sort_index()
frequency = 1
for last_trade_date, trade_date in zip(df_FundNav_t.index[:-frequency], df_FundNav_t.index[frequency:]):
    ser_FundReturn_t = df_FundNav_t.loc[[last_trade_date, trade_date]].pct_change().iloc[-1].dropna()
    ser_FundRank_t = ser_FundReturn_t.rank(ascending=False)
    # t1 = ser_FundRank_t['货币基金池'] / len(ser_FundRank_t)
    t2 = ser_FundRank_t['货币组合'] / len(ser_FundRank_t)
    # if t1 < 0.1:
    #     df_WeekRank.at[trade_date, '货币基金池'] = 1
    # else:
    #     df_WeekRank.at[trade_date, '货币基金池'] = 0

    if t2 < 0.1:
        df_WeekRank.at[trade_date, '货币组合'] = 1
    else:
        df_WeekRank.at[trade_date, '货币组合'] = 0
df_WeekRank_Rolling = df_WeekRank.rolling(52).apply(lambda x: np.sum(x)/52, 'raw=True')
df_WeekRank.loc['2019-03-03':].mean()
# 2019-3-3开始
# 稳健组合
bank_nav_t = bank_nav.append(bank_nav.reindex(trade_weeks)).sort_index().fillna(method='ffill').reindex(trade_weeks) / 100
df_SteadyReturn = df_Steady.reindex(trade_weeks).rolling(5).apply(lambda x: np.exp((np.log(x[4]) - np.log(x[0]))*12) - 1, 'raw=True')
df_CompareSteady = pd.merge(df_SteadyReturn, bank_nav_t[['mean']], how='inner', left_index=True, right_index=True).sort_index().rename(columns={'ra_nav': '稳健组合', 'mean': '银行'})
df_CompareSteady['win'] = df_CompareSteady['稳健组合'] - df_CompareSteady['银行']
df_CompareSteady.loc[df_CompareSteady.win < 0, 'win'] = 0.0
df_CompareSteady.loc[df_CompareSteady.win > 0, 'win'] = 1.0
df_SteadyRank_Rolling = df_CompareSteady[['win']].rolling(52, min_periods=4*3).mean()
df_SteadyRank_Rolling.rename(columns={'win': '稳健组合'}, inplace=True)

# lcmf
# df_Compare3Month = df_Risk10[['lcmf_risk10']].rename(columns={'lcmf_risk10': '智能组合'}).reindex(trade_weeks)
# df_3MonthRank = pd.DataFrame()
# df_FundNav_t = ra_fund_nav.reindex(dict_PoolCode['智能组合'], axis=1).copy()
# df_FundNav_t = pd.merge(df_FundNav_t, df_Compare3Month, how='inner', left_index=True, right_index=True).dropna(subset=['智能组合']).sort_index()
# frequency = 3*4
# for last_trade_date, trade_date in zip(df_FundNav_t.index[:-frequency], df_FundNav_t.index[frequency:]):
#     ser_FundReturn_t = df_FundNav_t.loc[[last_trade_date, trade_date]].pct_change().iloc[-1].dropna()
#     ser_FundRank_t = ser_FundReturn_t.rank(ascending=False)
#     t = ser_FundRank_t['智能组合'] / len(ser_FundRank_t)
#     if t < 0.5:
#         df_3MonthRank.at[trade_date, '智能组合'] = 1
#     else:
#         df_3MonthRank.at[trade_date, '智能组合'] = 0
#
# df_3MonthRank_Rolling = df_3MonthRank.rolling(52).apply(lambda x: np.sum(x)/52, 'raw=True')


# df_record = pd.concat([df_MonthRank_Rolling, df_WeekRank_Rolling, df_SteadyRank_Rolling, df_3MonthRank_Rolling], axis=1, join='outer')
df_record = pd.concat([df_MonthRank_Rolling, df_WeekRank_Rolling, df_SteadyRank_Rolling], axis=1, join='outer')
df_record = df_record.reindex(['货币组合', '稳健组合', '智能组合', '货币基金池', '债券基金池', 'a股基金池', '美股基金池', '港股基金池'], axis=1)

df_descriptor = pd.concat([df_record.mean().rename('均值'), df_record.max().rename('最大值'),
                           df_record.min().rename('最小值'), df_record.iloc[-1].rename('最新值')], axis=1).T

#########################################################################################################################
# 风险10
import numpy as np
import pandas as pd
import pymysql
begin_date = pd.Timestamp(2016, 8, 1)
end_date = pd.Timestamp(2019, 8, 15)
conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='asset_allocation', charset='utf8')
sql_t = 'select on_date, on_nav from on_online_nav where on_type=8 and on_online_id="800000"'
df_Risk10 = pd.read_sql(sql=sql_t, con=conn, index_col=['on_date'], parse_dates=['on_date']).rename(columns={'on_nav': 'lcmf_risk10'})
conn.close()
df_Risk10.at[pd.Timestamp(2019, 7, 22), 'lcmf_risk10'] = 2.0755
df_Risk10.at[pd.Timestamp(2019, 7, 23), 'lcmf_risk10'] = 2.0751
df_Risk10.at[pd.Timestamp(2019, 7, 24), 'lcmf_risk10'] = 2.0851
df_Risk10.at[pd.Timestamp(2019, 7, 25), 'lcmf_risk10'] = 2.0918
df_Risk10.at[pd.Timestamp(2019, 7, 26), 'lcmf_risk10'] = 2.0926
df_Risk10.at[pd.Timestamp(2019, 7, 29), 'lcmf_risk10'] = 2.0901
df_Risk10.at[pd.Timestamp(2019, 7, 30), 'lcmf_risk10'] = 2.0940
df_Risk10.at[pd.Timestamp(2019, 7, 31), 'lcmf_risk10'] = 2.0869
df_Risk10.at[pd.Timestamp(2019, 8, 1), 'lcmf_risk10'] = 2.0722
df_Risk10.at[pd.Timestamp(2019, 8, 2), 'lcmf_risk10'] = 2.0718
df_Risk10.at[pd.Timestamp(2019, 8, 5), 'lcmf_risk10'] = 2.0663
df_Risk10.at[pd.Timestamp(2019, 8, 6), 'lcmf_risk10'] = 2.0581
df_Risk10.at[pd.Timestamp(2019, 8, 7), 'lcmf_risk10'] = 2.0649
df_Risk10.at[pd.Timestamp(2019, 8, 8), 'lcmf_risk10'] = 2.0821
df_Risk10.at[pd.Timestamp(2019, 8, 9), 'lcmf_risk10'] = 2.0735
df_Risk10.at[pd.Timestamp(2019, 8, 15), 'lcmf_risk10'] = 2.0893
df_Risk10.at[pd.Timestamp(2019, 8, 16), 'lcmf_risk10'] = 2.0981
df_Risk10.at[pd.Timestamp(2019, 8, 22), 'lcmf_risk10'] = 2.1221
conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='mofang', charset='utf8')
sql_t = 'select ra_fund_id, ra_date, ra_nav_adjusted from ra_fund_nav'
ra_fund_nav = pd.read_sql(sql=sql_t, con=conn, parse_dates=['ra_date'])
ra_fund_nav.ra_fund_id = ra_fund_nav.ra_fund_id.astype(str)
ra_fund_nav = ra_fund_nav.drop_duplicates(subset=['ra_date', 'ra_fund_id']).set_index(['ra_date', 'ra_fund_id']).unstack()['ra_nav_adjusted'].sort_index()

sql_t = 'select * from yinhe_type'
yinhe_type = pd.read_sql(sql=sql_t, con=conn)
yinhe_type.yt_fund_id = yinhe_type.yt_fund_id.astype(str)

sql_t = 'select * from trade_dates'
df_t = pd.read_sql(con=conn, sql=sql_t, parse_dates=['td_date'], index_col=['td_date'])
trade_weeks = df_t.loc[((df_t.td_type & 0X02)>0) & (df_t.index > begin_date)].index.sort_values()
# notice
trade_weeks = trade_weeks.to_list()
trade_weeks[-1] = pd.Timestamp(2019,8,22)
conn.close()

# conn = pymysql.connect(host='192.168.88.12', user='public', passwd='h76zyeTfVqAehr5J', database='wind', charset='utf8')
# sql_t = 'select trade_days from asharecalendar where s_info_exchmarket = "SSE"'
# df_TradeDays = pd.read_sql(sql=sql_t, con=conn, parse_dates=['trade_days']).sort_values(by='trade_days')
# df_TradeDays = df_TradeDays.loc[(df_TradeDays.trade_days >= begin_date) & (df_TradeDays.trade_days <= end_date)]
# trade_days = pd.Index(df_TradeDays.trade_days)
# conn.close()
dict_PoolCode = dict()
df_ttt = yinhe_type.loc[yinhe_type.yt_l1_name.isin(['混合基金'])].copy()
codes_t = list(yinhe_type.loc[yinhe_type.yt_l1_name.isin(['股票基金']), 'yt_fund_id'].unique()) + list(df_ttt.loc[df_ttt.yt_l2_name.isin(['偏股型基金', '股债平衡型基金']), 'yt_fund_id'].unique())

dict_PoolCode['智能组合'] = set(codes_t)
yinhe_type_t = yinhe_type.loc[yinhe_type.yt_fund_id.isin(codes_t)].copy()

# lcmf
df_Compare3Month = df_Risk10[['lcmf_risk10']].rename(columns={'lcmf_risk10': '智能组合'}).reindex(trade_weeks)
df_3MonthRank = pd.DataFrame()
df_FundNav_t = ra_fund_nav.reindex(dict_PoolCode['智能组合'], axis=1).reindex(trade_weeks).copy()
df_FundNav_t = pd.merge(df_FundNav_t, df_Compare3Month, how='inner', left_index=True, right_index=True).dropna(subset=['智能组合']).sort_index()
frequency = 12
for last_trade_date, trade_date in zip(df_FundNav_t.index[:-frequency], df_FundNav_t.index[frequency:]):
    ser_FundReturn_t = df_FundNav_t.loc[[last_trade_date, trade_date]].pct_change().iloc[-1].dropna()
    ser_FundRank_t = ser_FundReturn_t.rank(ascending=False)
    t = ser_FundRank_t['智能组合'] / len(ser_FundRank_t)
    if t < 0.5:
        df_3MonthRank.at[trade_date, '智能组合'] = 1
    else:
        df_3MonthRank.at[trade_date, '智能组合'] = 0

df_3MonthRank_Rolling = df_3MonthRank.rolling(52).mean()