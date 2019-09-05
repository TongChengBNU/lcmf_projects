import pandas as pd
import database
from ipdb import set_trace

windGroup = {
    '20010104': '货币市场型基金'
}
begin_date = pd.Timestamp(2005, 1,1)
end_date = pd.Timestamp(2019,8, 28)

engine_wind = database.connection('wind')
engine_mofang = database.connection('mofang')
# runTime: < 1s
sql_tmp = "SELECT S_INFO_WINDCODE, F_PRT_ENDDATE, F_ANN_DATE, F_PRT_BONDTOTOT" +\
          ", F_PRT_CPVALUE, F_PRT_MTNVALUE, F_PRT_CORPBOND, F_PRT_TOTALASSET" +\
          " FROM ChinaMutualFundAssetPortfolio ORDER BY F_PRT_ENDDATE;"
# ChinaMutualFundAssetPortfolio
# Wind代码
# S_INFO_WINDCODE
# 截止日期
# F_PRT_ENDDATE
# 公告日期
# F_ANN_DATE
# 持有债券市值占资产总值比例( %)
# F_PRT_BONDTOTOT
# 持有短期融资券市值(元)
# F_PRT_CPVALUE
# 持有中期票据市值(元)
# F_PRT_MTNVALUE
# 持有企债市值(元)
# F_PRT_CORPBOND
# 资产总值(元)
# F_PRT_TOTALASSET
df_assets = pd.read_sql(sql=sql_tmp, con=engine_wind, parse_dates=['F_PRT_ENDDATE', 'F_ANN_DATE'])
df_assets = df_assets[ df_assets.F_PRT_ENDDATE >= begin_date ]
# pay attention to %%
sql_tmp = "SELECT F_INFO_WINDCODE, S_INFO_SECTORENTRYDT, S_INFO_SECTOREXITDT FROM chinamutualfundsector " +\
          "WHERE S_INFO_SECTOR LIKE '20010104%%';"
df_in_out = pd.read_sql(sql=sql_tmp.replace('\\',''), con=engine_wind, parse_dates=['S_INFO_SECTORENTRYDT', 'S_INFO_SECTOREXITDT'])
# 剔除代码中含有 ！的个体
df_in_out = df_in_out.loc[~df_in_out.F_INFO_WINDCODE.str.contains('!')].copy()

def Code(date):
    res = []
    for _, series in df_in_out.iterrows():
        if series['S_INFO_SECTORENTRYDT'] <= date:
            if  not (series['S_INFO_SECTOREXITDT'] < date):
                res.append(series['F_INFO_WINDCODE'])
    return res

sql_tmp = 'select * from trade_dates'
df_t = pd.read_sql(con=engine_mofang, sql=sql_tmp, parse_dates=['td_date'], index_col=['td_date'])
begin_date = pd.Timestamp(2010,1,1)
trade_months = df_t.loc[(df_t.td_type >= 8) & (df_t.index > begin_date)].index.sort_values()
df_trade_months = pd.DataFrame({'last_trade_month' : trade_months[:-1],
                                'trade_month' : trade_months[1:] }, columns=['last_trade_month', 'trade_month'])

def crossData(date: pd.Timestamp, sortKey = 'F_PRT_BONDTOTOT') -> pd.DataFrame:
    codeSet = Code(date)
    df_month = pd.DataFrame(columns=df_assets.columns, index=[date] * len(codeSet))
    for code, i in zip(codeSet, range(len(df_month))):
        df_assets_slice = df_assets[df_assets.S_INFO_WINDCODE == code].sort_values('F_ANN_DATE')
        try:
            df_assets_query = df_assets_slice[df_assets_slice.F_ANN_DATE < date].iloc[-1, :]
            df_month.iloc[i, :] = df_assets_query
        except IndexError:
            # print("Length of slice: %d ;" % len(df_assets_slice))
            # print("Index: %d ;" % i )
            continue
    df_month.dropna(axis=0, subset=['S_INFO_WINDCODE'], inplace=True)

    if len(codeSet) < 2:
        sql_t = "SELECT F_INFO_WINDCODE, ANN_DATE, PRICE_DATE, F_NAV_ADJUSTED FROM chinamutualfundnav WHERE F_INFO_WINDCODE IN " + str(tuple(codeSet)).replace(',', '') + ";"
    else:
        sql_t = "SELECT F_INFO_WINDCODE, ANN_DATE, PRICE_DATE, F_NAV_ADJUSTED FROM chinamutualfundnav WHERE F_INFO_WINDCODE IN " + str(tuple(codeSet)) + ";"
    df_nav = pd.read_sql(sql=sql_t, con=engine_wind, parse_dates=['ANN_DATE', 'PRICE_DATE'])

    df_month['RETURN'] = pd.Series(None)
    def snippet(series):
        df_date_interval_query = df_trade_months[df_trade_months.last_trade_month >= series.F_ANN_DATE].iloc[-1, :]
        try:
            series_tmp = df_nav[(df_nav.F_INFO_WINDCODE == series.S_INFO_WINDCODE) & (df_nav.PRICE_DATE >= df_date_interval_query.iloc[0]) & (df_nav.PRICE_DATE <= df_date_interval_query.iloc[1])].F_NAV_ADJUSTED.sort_values().pct_change()
            series.RETURN = (series_tmp.fillna(0) + 1).cumprod().iloc[-1] - 1
        except IndexError:
            return series
        return series
    df_month = df_month.apply(func=snippet, axis=1)

    df_rank_month = pd.DataFrame(columns=['S_INFO_WINDCODE', 'F_PRT_BONDTOTOT', 'F_PRT_THREE', 'RETURN'])
    # df_month.F_PRT_CP_PERCENTAGE = df_month.F_PRT_CPVALUE / df_month.F_PRT_TOTALASSET * 100
    # df_month.F_PRT_MTN_PERCENTAGE = df_month.F_PRT_MTNVALUE / df_month.F_PRT_TOTALASSET * 100
    # df_month.F_PRT_CORP_PERCENTAGE = df_month.F_PRT_CORPBOND / df_month.F_PRT_TOTALASSET * 100

    df_rank_month.S_INFO_WINDCODE = df_month.S_INFO_WINDCODE
    df_rank_month.F_PRT_BONDTOTOT = df_month.F_PRT_BONDTOTOT
    df_rank_month.F_PRT_THREE = df_month.F_PRT_CPVALUE / df_month.F_PRT_TOTALASSET * 100 + df_month.F_PRT_MTNVALUE / df_month.F_PRT_TOTALASSET * 100 + df_month.F_PRT_CORPBOND / df_month.F_PRT_TOTALASSET * 100
    df_rank_month.RETURN = df_month.RETURN
    df_rank_month.sort_values(sortKey, inplace=True, ascending=False)
    batch = min([20, len(df_rank_month)])
    df_portfolio_return = df_rank_month.iloc[:batch, :].RETURN.mean()
    df_total_return = df_rank_month.RETURN.median()
    return df_portfolio_return, df_total_return

# date = pd.Timestamp(2019, 3, 31)
# sortKey = 'F_PRT_BONDTOTOT'
# df_month = crossData(date)
# df_month = tmp


if __name__ == "__main__":
    dfCore = pd.DataFrame(index=trade_months, columns=['portfolio', 'total'])
    for index in dfCore.index:
        try:
            portfolio_return, total_return  = crossData(date=index, sortKey= 'F_PRT_BONDTOTOT')
            dfCore.loc[index, 'portfolio'] = portfolio_return
            dfCore.loc[index, 'total'] = total_return
        except IndexError:
            print("Error Date: %s" % str(index))

    dfCore.to_csv('./cashportfolio.csv')




























