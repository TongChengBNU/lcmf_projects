#encoding=utf8


import numpy as np
import pandas as pd

from ipdb import set_trace

from sqlalchemy.orm import sessionmaker
from db import database
from db import asset_allocate
import trade_date

engine_asset = database.connection('asset')
engine_mofang = database.connection('base')
engine_readonly = database.connection('readonly')
Session = sessionmaker(bind=engine_asset)

begin_date = pd.Timestamp(2016,8,12)
end_date = pd.Timestamp(2019,8,23)

#a_trade_date = trade_date.ATradeDate()
#trade_weeks = a_trade_date.week_trade_date(begin_date=begin_date, end_date=end_date)
def TradeWeeks(engine=engine_mofang):
    sql_t = 'select * from trade_dates'
    df_t = pd.read_sql(con=engine, sql=sql_t, parse_dates=['td_date'], index_col=['td_date'])
    trade_weeks = df_t.loc[((df_t.td_type & 0X02)>0) & (df_t.index > begin_date)].index.sort_values()
    # notice
    trade_weeks = list(trade_weeks)
    trade_weeks[-1] = pd.Timestamp(2019, 8, 22)
    return trade_weeks
trade_weeks = TradeWeeks()



def Risk10(engine=engine_asset):
    sql_tmp = 'select ra_date, ra_portfolio_id, ra_nav from ra_portfolio_nav where ra_type = 8;'
    df_Risk10 = pd.read_sql(sql=sql_tmp, con=engine, parse_dates=['ra_date'])
    ids = ['PO.OnlineAShare', 'PO.OnlineMShare', 'PO.OnlineHShare', 'PO.OnlineShare', 'PO.OnlineBond', 'PO.OnlineCommodity']
    df_Risk10 = df_Risk10.set_index(['ra_date', 'ra_portfolio_id']).unstack()['ra_nav'].reindex(ids, axis=1)
    return df_Risk10


def CashPortfolio(engine=engine_asset):
    sql_tmp = "SELECT ra_date, ra_nav FROM ra_pool_nav WHERE ra_pool = '11310102';"
    df_CashPortfolio = pd.read_sql(sql=sql_tmp, con=engine, parse_dates=['ra_date'], index_col=['ra_date'])
    return df_CashPortfolio


def Steady(engine=engine_asset):
    sql_tmp = "SELECT ra_date, ra_nav FROM ra_portfolio_nav WHERE ra_portfolio_id = 'PO.CB0040' AND ra_date >= '2018-01-01';"
    df_Steady = pd.read_sql(sql=sql_tmp, con=engine, parse_dates=['ra_date'])
    df_Steady.drop_duplicates(subset=['ra_date'], keep='first', inplace=True) 
    df_Steady.set_index('ra_date', inplace=True)
    return df_Steady


# Slow, 8640 columns
def Fund_nav(engine=engine_mofang):
    sql_t = "select ra_fund_id, ra_date, ra_nav_adjusted from ra_fund_nav WHERE ra_date >= '2015-01-01';"
    ra_fund_nav = pd.read_sql(sql=sql_t, con=engine, parse_dates=['ra_date'])
    ra_fund_nav.ra_fund_id = ra_fund_nav.ra_fund_id.astype(str)
    ra_fund_nav = ra_fund_nav.drop_duplicates(subset=['ra_date', 'ra_fund_id']).set_index(['ra_date', 'ra_fund_id']).unstack()['ra_nav_adjusted'].sort_index()
    return ra_fund_nav


def Yinhe_type(engine=engine_readonly):
    sql_t = 'select * from yinhe_type;'
    yinhe_type = pd.read_sql(sql=sql_t, con=engine)
    yinhe_type.yt_fund_id = yinhe_type.yt_fund_id.astype(str)
    return yinhe_type


def Bank_nav(engine=engine_asset):
    sql_tmp = "SELECT date, annual_return AS mean FROM financial_product_bank;"
    bank_nav = pd.read_sql(sql=sql_tmp, con=engine, parse_dates=['date'], index_col=['date'])
    return bank_nav
    ## 银行理财
    #bank_nav.set_index('date', inplace=True)
    ## 两种商业银行做均值
    #bank_nav['mean'] = bank_nav.mean(axis=1)


def dingkai(engine=engine_asset):
    sql_tmp = "SELECT stock_code, stock_name FROM dingkaizhaiji;"
    dingkai = pd.read_sql(sql=sql_tmp, con=engine)
    dingkai['code'] = dingkai['stock_code'].apply(lambda x: x[:-3])
    return dingkai


def PoolCode():
    yinhe_type = Yinhe_type()

    dict_PoolCode = dict()
    pool_str = ['智能组合', '货币基金池', '债券基金池', 'a股基金池', '美股基金池', '港股基金池']
    # t = ['中短期标准债券型基金', '中短期标准债券型基金(B/C类)', '长期标准债券型基金(A类)', '长期标准债券型基金(B/C类)', '指数债券型基金(A类)', '指数债券型基金(B/C类)']
    t = ['中短期标准债券型基金', '中短期标准债券型基金(B/C类)', '长期标准债券型基金(B/C类)', '指数债券型基金(B/C类)']
    BondFund_t = yinhe_type.loc[yinhe_type.yt_l3_name.isin(t)].copy()

    df_ttt = yinhe_type.loc[yinhe_type.yt_l1_name.isin(['混合基金'])].copy()
    codes_t = list(yinhe_type.loc[yinhe_type.yt_l1_name.isin(['股票基金']), 'yt_fund_id'].unique()) + list(df_ttt.loc[df_ttt.yt_l2_name.isin(['偏股型基金', '股债平衡型基金']), 'yt_fund_id'].unique())

    # Code 不能包含重复值
    dict_PoolCode['智能组合'] = set(codes_t)

    dict_PoolCode['货币组合'] = yinhe_type.loc[yinhe_type.yt_l1_name =='货币市场基金', 'yt_fund_id'].unique()
    dict_PoolCode['债券基金池'] = BondFund_t.yt_fund_id.unique()
    dict_PoolCode['a股基金池'] = yinhe_type.loc[yinhe_type.yt_l1_name =='股票基金', 'yt_fund_id'].unique()
    dict_PoolCode['美股基金池'] = yinhe_type.loc[yinhe_type.yt_l2_name =='QDII股票基金', 'yt_fund_id'].unique()
    dict_PoolCode['港股基金池'] = yinhe_type.loc[yinhe_type.yt_l2_name =='QDII股票基金', 'yt_fund_id'].unique()
    return dict_PoolCode



# 四种基金池: A股, 美股, 港股, 债券
def MonthRank_Rolling():
    df_Risk10 = Risk10()
    ra_fund_nav = Fund_nav()
    dict_PoolCode = PoolCode()

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
    return df_MonthRank_Rolling


# 货币组合
def WeekRank_Rolling():
    df_CashPortfolio = CashPortfolio()
    ra_fund_nav = Fund_nav()
    dict_PoolCode = PoolCode()

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

        if t2 < 0.2:
            df_WeekRank.at[trade_date, '货币组合'] = 1
        else:
            df_WeekRank.at[trade_date, '货币组合'] = 0

    df_WeekRank_Rolling = df_WeekRank.loc['2019-01-01':].rolling(52, min_periods=10).apply(lambda x: np.sum(x)/52, 'raw=True')
   # print('df_WeekRank 2019-03-03 MEAN: %f' % df_WeekRank.loc['2019-03-03':].mean())
    return df_WeekRank_Rolling


# 稳健组合 
def SteadyRank_Rolling():
    bank_nav = Bank_nav()
    df_Steady = Steady()

    # 2019-3-3开始
    # 稳健组合
    bank_nav_t = bank_nav.append(bank_nav.reindex(trade_weeks)).sort_index().fillna(method='ffill').reindex(trade_weeks) / 100
    df_SteadyReturn = df_Steady.reindex(trade_weeks).rolling(5).apply(lambda x: np.exp((np.log(x[4]) - np.log(x[0]))*12) - 1, 'raw=True')
    df_CompareSteady = pd.merge(df_SteadyReturn, bank_nav_t[['mean']], how='inner', left_index=True, right_index=True).sort_index().rename(columns={'ra_nav': '稳健组合', 'mean': '银行'})
    df_CompareSteady['win'] = df_CompareSteady['稳健组合'] - df_CompareSteady['银行']
    df_CompareSteady.loc[df_CompareSteady.win < 0, 'win'] = 0.0
    df_CompareSteady.loc[df_CompareSteady.win > 0, 'win'] = 1.0
    df_SteadyRank_Rolling = df_CompareSteady[['win']].rolling(window=52, min_periods=4*3).mean()
    df_SteadyRank_Rolling.rename(columns={'win': '稳健组合'}, inplace=True)
    return df_SteadyRank_Rolling


# 智能组合
def Month_3_Rank_Rolling():
    dict_PoolCode = PoolCode()

    sql_t = 'select on_date, on_nav from on_online_nav where on_type=8 and on_online_id="800000"'
    df_Risk10 = pd.read_sql(sql=sql_t, con=engine_asset, index_col=['on_date'], parse_dates=['on_date']).rename(columns={'on_nav': 'lcmf_risk10'})

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

    sql_t = 'select ra_fund_id, ra_date, ra_nav_adjusted from ra_fund_nav'
    ra_fund_nav = pd.read_sql(sql=sql_t, con=engine_mofang, parse_dates=['ra_date'])
    ra_fund_nav.ra_fund_id = ra_fund_nav.ra_fund_id.astype(str)
    ra_fund_nav = ra_fund_nav.drop_duplicates(subset=['ra_date', 'ra_fund_id']).set_index(['ra_date', 'ra_fund_id']).unstack()['ra_nav_adjusted'].sort_index()

    sql_t = 'select * from yinhe_type'
    yinhe_type = pd.read_sql(sql=sql_t, con=engine_mofang)
    yinhe_type.yt_fund_id = yinhe_type.yt_fund_id.astype(str)

    sql_t = 'select * from trade_dates'
    df_t = pd.read_sql(sql=sql_t,con=engine_mofang,  parse_dates=['td_date'], index_col=['td_date'])

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
    return df_3MonthRank_Rolling










def update():
    currency = criteria_ct.WeekRank_Rolling()
    steady = criteria_ct.SteadyRank_Rolling()
    intelligent = criteria_ct.Month_3_Rank_Rolling()
    pool = criteria_ct.MonthRank_Rolling()

    df = pd.concat([currency, steady, intelligent, pool], axis=1, join='outer')
    engine = database.connection('asset')
    Session = sessionmaker(bind=engine)
    session = Session()
    for i in range(len(df)):
        ins = asset_allocate.criteria(
                    date = df.index[i],

                    cash_portfolio = df.iloc[i,0],
                    steady_portfolio = df.iloc[i,1],
                    intelligent_portfolio = df.iloc[i,2],
                    a_stock_pool = df.iloc[i,3],
                    US_stock_pool = df.iloc[i,4],
                    HK_stock_pool = df.iloc[i,5],
                    bond_pool = df.iloc[i,6]
                )
        session.add(ins)
        session.commit()
        print("成功插入了 %d 条数据;" % (i+1))
    return


