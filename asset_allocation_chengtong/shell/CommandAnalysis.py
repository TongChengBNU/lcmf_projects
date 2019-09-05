#coding=utf8


import string
import os
import sys
sys.path.append('shell')
import click
import pandas as pd
import numpy as np
import os
import time
import logging
import re
import MySQLdb
import config


from datetime import datetime, timedelta
from dateutil.parser import parse
from Const import datapath
from sqlalchemy import MetaData, Table, select, func, literal_column
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate
from db import database, base_exchange_rate_index, base_ra_index, asset_ra_pool_fund, base_ra_fund, asset_ra_pool, asset_on_online_nav, asset_ra_portfolio_nav, asset_on_online_fund, asset_mz_markowitz_nav, base_ra_index_nav, asset_ra_composite_asset_nav, base_exchange_rate_index_nav, base_ra_fund_nav, asset_mz_highlow_pos, asset_ra_pool_nav, asset_allocate
from util import xdict
from trade_date import ATradeDate

import traceback, code

logger = logging.getLogger(__name__)

@click.group(invoke_without_command=True)
@click.pass_context
def analysis(ctx):
    '''
        analysis something
    '''
    pass

@analysis.command()
@click.pass_context
def helloworld(ctx):
    print('Hello World')



@analysis.command()
@click.pass_context
@click.option('--start-date', 'st_date', default=None, help='portfolio pos startdate')
@click.option('--end-date', 'ed_date', default=None, help='portfolio pos endate')
def macroview_retcompare(ctx,st_date,ed_date):
    startDate = parse(st_date)
    endDate  = parse(ed_date)
    assetsID = {'120000001':'沪深300','120000002':'中证500','120000013':'标普500','120000015':'恒生指数','120000014':'沪金指数','120000010':'中证国债','120000011':'中证信用债'}
    #assetsRet = {'120000001':0.2743,'120000002':0.3576,'120000013':0.0953,'120000015':0.0997,'120000014':-0.0029,'120000010':0.0009,'120000011':0.0032}
    assets = dict([(asset_id, base_ra_index_nav.load_series(asset_id)) for asset_id in list(assetsID.keys())])
    df_assets = pd.DataFrame(assets).loc[startDate:endDate,].fillna(method='pad')
    assetsRet = dict([(asset_id,df_assets.loc[endDate,asset_id] / df_assets.loc[startDate,asset_id] - 1.0) for asset_id in list(assetsID.keys())])
     #     #计算给定指数和日期的持有期收益
    df_assets = pd.DataFrame(assets)
    df_assets = df_assets.rolling(365).apply(lambda x : x[-1] / x[0] - 1,raw=True)
    df = df_assets.reset_index()
    tradedaysDiff = df[df.date==endDate].index.tolist()[0] - df[df.date==startDate].index.tolist()[0]
    annualMulty = 365/tradedaysDiff
    MacroCompare = []
    for key,value in assetsRet.items():
        values = value*annualMulty
        ser = df_assets[key].dropna()
        ser = ser.sort_values()
        if len(ser) == 0:
            continue
        MacroCompare.append((assetsID[key], 1 - len(ser[ser < values]) / len(ser)))
    df_MacroCompare = pd.DataFrame(MacroCompare,columns = ['ra_index','分位数'])
    #print(df_MacroCompare)
    ser = pd.Series(assetsRet)
    ret_df = pd.DataFrame(columns = ser.index)
    ret_df.loc[endDate] = ser
    ret_df = ret_df.rename(columns = assetsID)
    print(ret_df)
    df_MacroCompare['date'] = endDate
    df_MacroCompare = df_MacroCompare.set_index(['date','ra_index']).unstack()
    df_MacroCompare.columns = df_MacroCompare.columns.get_level_values(1)
    df_MacroCompare = df_MacroCompare[ret_df.columns]
    df_MacroCompare.columns = df_MacroCompare.columns + ' 分位数'
    df = pd.concat([ret_df, df_MacroCompare] ,axis = 1)
    print(df)
    df.to_csv('宏观观点数据.csv' ,encoding='gbk')



#基金池收益率排名
@analysis.command()
@click.pass_context
def pool_rank(ctx):


    engine = database.connection('base')
    Session = sessionmaker(bind=engine)
    session = Session()
    sql_t = 'select ra_fund_id, ra_date, ra_nav_adjusted from ra_fund_nav where ra_date > "2018-01-01"'
    ra_fund_nav = pd.read_sql(sql=sql_t, con=session.bind)
    ra_fund_nav.ra_fund_id = ra_fund_nav.ra_fund_id.astype(str)
    session.commit()
    session.close()

    engine = database.connection('base')
    Session = sessionmaker(bind=engine)
    session = Session()
    sql_t = 'select * from yinhe_type'
    yinhe_type = pd.read_sql(sql=sql_t, con=session.bind)
    yinhe_type.yt_fund_id = yinhe_type.yt_fund_id.astype(str)
    session.commit()
    session.close()

    engine = database.connection('asset')
    Session = sessionmaker(bind=engine)
    session = Session()
    sql_t = 'select ra_pool, ra_date, ra_nav from ra_pool_nav where ra_date > "2018-01-01"'
    ra_pool_nav = pd.read_sql(sql=sql_t, con=session.bind)
    ra_pool_nav.ra_pool = ra_pool_nav.ra_pool.astype(str)
    ra_pool_nav.ra_date = pd.to_datetime(ra_pool_nav.ra_date)
    ra_pool_nav.set_index('ra_date', inplace=True)
    session.commit()
    session.close()

    columns_debt = ['中短期标准债券型基金', '中短期标准债券型基金(B/C类)', '长期标准债券型基金(A类)', '长期标准债券型基金(B/C类)', '指数债券型基金(A类)', '指数债券型基金(B/C类)']
    loc_t_debt = yinhe_type.yt_l3_name.isin(columns_debt)

    columns_stock = ['指数股票型基金', '标准股票型基金', '行业股票型基金', '股票型分级子基金', '偏股型基金']
    loc_t_stock = yinhe_type.yt_l2_name.isin(columns_stock)
    #
    columns_cash = ['货币市场基金']
    loc_t_cash = yinhe_type.yt_l1_name.isin(columns_cash)

    rank_record = pd.DataFrame()
    for i_num in range(3):
        if i_num == 0:
            loc_t = loc_t_debt
            ra_pool = ['11210100', '11210200']
        elif i_num == 1:
            loc_t = loc_t_stock
            ra_pool = ['11110116', '11110114', '11110112', '11110110', '11110108', '11110106', '11110100', '11110200']
        else:
            loc_t = loc_t_cash
            ra_pool = ['11310102']

        yinhe_type_t = yinhe_type.loc[loc_t].copy()
        codes_list = list(yinhe_type_t.yt_fund_id.values)

        ra_fund_nav_t = ra_fund_nav.loc[ra_fund_nav.ra_fund_id.isin(codes_list)].copy()
        ra_fund_nav_t.ra_date = pd.to_datetime(ra_fund_nav_t.ra_date)
        ra_fund_nav_t.set_index('ra_date', inplace=True)
        ra_pool_nav_t = ra_pool_nav.loc[ra_pool_nav.ra_pool.isin(ra_pool)].copy()
        dates = pd.date_range(datetime.now().date() - timedelta(400) , datetime.now().date())
        dates = dates[0:-1]
        #print(dates)
        date_list_begin = [dates[-2].strftime('%Y-%m-%d'), dates[-7].strftime('%Y-%m-%d'), dates[-31].strftime('%Y-%m-%d'), dates[-91].strftime('%Y-%m-%d'), dates[-182].strftime('%Y-%m-%d') , dates[-365].strftime('%Y-%m-%d')]
        date_end = dates[-1].strftime('%Y-%m-%d')
        ra_pool_nav_t = ra_pool_nav_t.reset_index()
        ra_pool_nav_t = ra_pool_nav_t.set_index(['ra_date', 'ra_pool'])
        ra_pool_nav_t = ra_pool_nav_t.unstack().fillna(method='pad')
        ra_pool_nav_t = ra_pool_nav_t.reindex(dates).fillna(method='pad')
        ra_pool_nav_t = ra_pool_nav_t.stack()

        # 截至日期排名
        for j_num, j_date in enumerate(date_list_begin):
            ra_fund_nav_t0 = ra_fund_nav_t.loc[j_date].copy()
            ra_fund_nav_t1 = ra_fund_nav_t.loc[date_end].copy()
            ra_fund_nav_t01 = pd.merge(ra_fund_nav_t0, ra_fund_nav_t1, how='inner', on='ra_fund_id', sort=False, suffixes=('_0', '_1'))
            ra_fund_nav_t01['NEXT_RETURN'] = (ra_fund_nav_t01['ra_nav_adjusted_1'] / ra_fund_nav_t01['ra_nav_adjusted_0']) - 1
            #
            ra_pool_nav_t0 = ra_pool_nav_t.loc[j_date:j_date, :].copy()
            ra_pool_nav_t1 = ra_pool_nav_t.loc[date_end:date_end, :].copy()
            ra_pool_nav_t01 = pd.merge(ra_pool_nav_t0, ra_pool_nav_t1, how='inner', on='ra_pool', sort=False, suffixes=('_0', '_1'))
            ra_pool_nav_t01['NEXT_RETURN'] = (ra_pool_nav_t01['ra_nav_1'] / ra_pool_nav_t01['ra_nav_0']) - 1
            #
            ra_fund_nav_add = ra_fund_nav_t01.iloc[0:1, :].copy()
            ra_fund_nav_add['ra_fund_id'] = 'select'
            ra_fund_nav_add['NEXT_RETURN'] = ra_pool_nav_t01['NEXT_RETURN'].mean()
            #
            ra_fund_nav_t01 = ra_fund_nav_t01.append(ra_fund_nav_add, ignore_index=True)
            ra_fund_nav_t01['rank'] = ra_fund_nav_t01['NEXT_RETURN'].rank(method='first', ascending=False)
            ra_fund_nav_t01.set_index('ra_fund_id', inplace=True)

            rank_record.at[j_date, str(i_num)+'_rank'] = ra_fund_nav_t01.at['select', 'rank']
            rank_record.at[j_date, str(i_num)+'_samples'] = ra_fund_nav_t01.shape[0] - 1
            rank_record.at[j_date, str(i_num)+'_return'] = ra_pool_nav_t01['NEXT_RETURN'].mean()

    df = pd.DataFrame(index = [date_end + ' 当日', date_end + ' 过去一周',date_end + ' 过去一月',date_end + ' 过去三个月',date_end + ' 过去六个月',date_end + ' 过去一年'])
    df['债券收益均值'] = rank_record['0_return'].ravel()
    df['债券收益排名'] = (rank_record['0_rank'].astype(int).astype(str)+ '/' + rank_record['0_samples'].astype(int).astype(str)).ravel()
    df['股票收益均值'] = rank_record['1_return'].ravel()
    df['股票收益排名'] = (rank_record['1_rank'].astype(int).astype(str)+ '/' + rank_record['1_samples'].astype(int).astype(str)).ravel()
    df['货币收益均值'] = rank_record['2_return'].ravel()
    df['货币收益排名'] = (rank_record['2_rank'].astype(int).astype(str)+ '/' + rank_record['2_samples'].astype(int).astype(str)).ravel()
    print(df)
    df.to_csv('基金池收益排名.csv', encoding='gbk')


#组合和标杆组合,收益率对比
@analysis.command()
@click.pass_context
def allocate_benchmark_comp(ctx):

    index_ids = ['120000016', '120000010']
    data = {}
    for _id in index_ids:
        data[_id] = base_ra_index_nav.load_series(_id)
    df = pd.DataFrame(data)

    composite_asset_ids = ['20201','20202', '20203', '20204', '20205', '20206', '20207', '20208']

    data = {}

    for _id in composite_asset_ids:
        nav = asset_ra_composite_asset_nav.load_nav(_id)
        nav = nav.reset_index()
        nav = nav[['ra_date', 'ra_nav']]
        nav = nav.set_index(['ra_date'])
        data[_id] = nav.ra_nav

    bench_df = pd.DataFrame(data)
    benchmark_df = pd.concat([bench_df,df],axis = 1, join_axes = [bench_df.index])

    conn  = MySQLdb.connect(**config.db_asset)
    conn.autocommit(True)

    dfs = []
    for i in range(0, 10):
        sql = 'select on_date as date, on_nav as nav from on_online_nav where on_online_id = 80000%d and on_type = 8' % i
        df = pd.read_sql(sql, conn, index_col = ['date'], parse_dates = ['date'])
        df.columns = ['risk_' + str(i)]
        dfs.append(df)

    df = pd.concat(dfs, axis = 1)

    conn.close()

    df = pd.concat([df, benchmark_df], axis = 1, join_axes = [df.index])
    df = df.fillna(method='pad')
    df = df.rename(columns = {'risk_0':'风险10','risk_1':'风险1','risk_2':'风险2','risk_3':'风险3','risk_4':'风险4','risk_5':'风险5',
                            'risk_6':'风险6','risk_7':'风险7','risk_8':'风险8','risk_9':'风险9',
                            '20201':'风险2比较基准','20202':'风险3比较基准', '20203':'风险4比较基准', '20204':'风险5比较基准', 
                            '20205':'风险6比较基准', '20206':'风险7比较基准', '20207':'风险8比较基准', '20208':'风险9比较基准',
                            '120000016':'风险10比较基准','120000010':'风险1比较基准'})
    cols = ['风险1', '风险2', '风险3', '风险4', '风险5', '风险6', '风险7', '风险8', '风险9', '风险10','风险1比较基准','风险2比较基准', '风险3比较基准', '风险4比较基准', '风险5比较基准', '风险6比较基准', '风险7比较基准', '风险8比较基准', '风险9比较基准', '风险10比较基准']
    df = df[cols]

    result_df = pd.DataFrame(columns = df.columns)
    last_day = df.index[-1]
    result_df.loc[df.index[-1].strftime('%Y-%m-%d') + ' 当日'] = df.pct_change().iloc[-1]
    result_df.loc[df.index[-1].strftime('%Y-%m-%d') + ' 过去一周'] = df.loc[last_day] / df.loc[last_day - timedelta(weeks = 1)] - 1
    result_df.loc[df.index[-1].strftime('%Y-%m-%d') + ' 过去一月'] = df.loc[last_day] / df.loc[last_day - timedelta(days = 31)] - 1
    result_df.loc[df.index[-1].strftime('%Y-%m-%d') + ' 过去三个月'] = df.loc[last_day] / df.loc[last_day - timedelta(days = 91)] - 1
    result_df.loc[df.index[-1].strftime('%Y-%m-%d') + ' 过去六个月'] = df.loc[last_day] / df.loc[last_day - timedelta(days = 182)] - 1
    result_df.loc[df.index[-1].strftime('%Y-%m-%d') + ' 过去一年'] = df.loc[last_day] / df.loc[last_day - timedelta(days = 365)] - 1
    result_df.to_csv('智能组合收益与比较基准收益比较.csv', encoding='gbk')

