import pymysql
import pandas as pd
from sqlalchemy import create_engine

def DBconnection(name):
    if name == 'wind':
        conn_wind = pymysql.connect(host='192.168.88.12', user='public', passwd='h76zyeTfVqAehr5J', database='wind',charset='utf8')
        return conn_wind
    elif name == 'localhost':
        conn_local = pymysql.connect(host='localhost', user='root', passwd='chengtong123', database='finance',
                                     charset='utf8')
        return conn_local
    else:
        print('Failed;')
        return



def windTolocalhost():
    # conn_wind = pymysql.connect(host='192.168.88.12', user='public', passwd='h76zyeTfVqAehr5J', database='wind', charset='utf8')
    # conn_local = pymysql.connect(host='localhost', user='root', passwd='chengtong123', database='finance', charset='utf8')
    conn_wind = DBconnection(name='wind')
    conn_local = DBconnection(name='localhost')


    cursor_wind = conn_wind.cursor()
    sql_wind = "SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_CLOSE, S_DQ_CHANGE, S_DQ_PCTCHANGE FROM aindexeodprices WHERE S_INFO_WINDCODE = 'H00906.CSI';"
    len1 = cursor_wind.execute(sql_wind)


    cursor_local = conn_local.cursor()
    sql_local = "INSERT INTO zhongzheng800quan(S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_CLOSE, S_DQ_CHANGE, S_DQ_PCTCHANGE) VALUE(%s, %s, %s, %s, %s, %s);"




    num = 11

    for i in range(int(len1/num)):
        print(i)
        data1 = cursor_wind.fetchmany(num)
        cursor_local.executemany(sql_local, data1)


    data2 = cursor_wind.fetchall()
    cursor_local.executemany(sql_local, data2)

    conn_local.commit()

    conn_wind.close()
    conn_local.close()
    return



# path = "E:/1-Career/lcmf/factorModel/Shibor3M.xls"
# shibor_df = pd.read_excel(path, parse_dates=['date'])
# shibor_df.set_index('date', inplace=True)

# conn_localhost = DBconnection(name='localhost')
# engine = create_engine("mysql://root:chengtong123@localhost/macroindex", encoding='utf8')
# shibor_df.to_sql(name='macroindex', con=engine, if_exists='append', index=True)


def Set2():
    '''插入信用利差和shibor

    :return:
    '''
    path = "E:/1-Career/lcmf/Projects/ERP/"
    file1, file2 = ('信用利差AAA和AA.xls', '信用利差(中位数)产业债不同评级.xls')
    df_short = pd.read_excel(path+file1, parse_dates=['date'])
    df_long = pd.read_excel(path+file2, parse_dates=['date'])

    # df_short.set_index('date', inplace=True)
    # df_long.set_index('date', inplace=True)

    # df_short
    dfCore = pd.merge(left=df_short, right=df_long, on='date', how='outer')
    dfCore.set_index('date', inplace=True)
    dfCore.sort_index(inplace=True)

    conn_local = DBconnection('localhost')
    sql = "SELECT * FROM macroindex;"
    df_ref = pd.read_sql(sql=sql, con=conn_local, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
    dateSet = df_ref.index

    cursor= conn_local.cursor()
    sql_insert = "INSERT INTO `macroindex` VALUES ('{DT}', NULL, NULL, NULL, {Shibor}, {CS3A}, {CS2A});"
    sql_update = "UPDATE `macroindex` " \
                 "SET `Shibor_3M`= {Shibor}, `CreditSpread_3A`={CS3A}, `CreditSpread_2A`= {CS2A} " \
                 "WHERE `TRADE_DT`= '{DT}';"
    count = 0


    for index, srs in dfCore.iterrows():
        # print(index, type(srs))
        # print(index in dateSet)
        args_dict = {
            'Shibor': srs.iloc[3],
            'CS3A': srs.iloc[1],
            'CS2A': srs.iloc[2],
            'DT': str(index)[:10]
        }

        if (index in dateSet):
            cursor.execute(sql_update.format(**args_dict).replace('nan','NULL'))
        else:
            cursor.execute(sql_insert.format(**args_dict).replace('nan','NULL'))

        conn_local.commit()
        count += 1
        print('Insert %d queries successfully;' % count)


def Set3():
    '''插入中证500全收益数据

    :return:
    '''
    path = "E:/1-Career/lcmf/Projects/ERP/"
    # file1, file2 = ('信用利差AAA和AA.xls', '信用利差(中位数)产业债不同评级.xls')
    file = '中证500全收益.xls'
    df_short = pd.read_excel(path + file, parse_dates=['TRADE_DT'])
    # df_long = pd.read_excel(path + file2, parse_dates=['date'])


    dfCore = pd.DataFrame(
        {
            'S_INFO_WINDCODE': df_short['S_INFO_WINDCODE'],
            'TRADE_DT': df_short['TRADE_DT'],
            'S_DQ_PRECLOSE': df_short['S_DQ_PRECLOSE'],
            'S_DQ_CLOSE': df_short['S_DQ_CLOSE'],
            'S_DQ_CHANGE': df_short['S_DQ_CHANGE'],
            'S_DQ_PCTCHANGE': df_short['S_DQ_PCTCHANGE']
        }
    )
    dfCore.set_index('TRADE_DT', inplace=True)
    dfCore.sort_index(inplace=True)
    dfCore['S_DQ_PCTCHANGE'] = dfCore['S_DQ_PCTCHANGE'].apply(lambda x: 1 if x>0 else 0)

    conn_local = DBconnection('localhost')
    sql = "SELECT * FROM aindexeodprices;"
    df_ref = pd.read_sql(sql=sql, con=conn_local, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
    dateSet = df_ref.index

    cursor = conn_local.cursor()
    sql_insert = "INSERT INTO `aindexeodprices` VALUES ('{code}', '{DT}', {pre}, {close}, {change}, {pct});"
    # sql_update = "UPDATE `macroindex` " \
    #              "SET `Shibor_3M`= {Shibor}, `CreditSpread_3A`={CS3A}, `CreditSpread_2A`= {CS2A} " \
    #              "WHERE `TRADE_DT`= '{DT}';"
    count = 0

    for index, srs in dfCore.iterrows():
        # print(index, type(srs))
        # print(index in dateSet)
        args_dict = {
            'code': srs.iloc[0],
            'DT': str(index)[:10].replace('-', ''),
            'pre': srs.iloc[1],
            'close': srs.iloc[2],
            'change': srs.iloc[3],
            'pct': srs.iloc[4]
        }

        # if (index in dateSet):
        #     cursor.execute(sql_update.format(**args_dict).replace('nan', 'NULL'))
        # else:
        cursor.execute(sql_insert.format(**args_dict).replace('nan', 'NULL'))

        conn_local.commit()
        count += 1
        print('Insert %d queries successfully;' % count)



def Set4():
    '''插入 GDP:mainland

    :return:
    '''
    path = "E:/1-Career/lcmf/Projects/ERP/"
    file = 'GDP.xls'
    df = pd.read_excel(path + file, parse_dates=['date'])

    # df_short
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    conn_local = DBconnection('localhost')
    sql = "SELECT `TRADE_DT` FROM macroindex;"
    df_ref = pd.read_sql(sql=sql, con=conn_local, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
    dateSet = df_ref.index

    cursor = conn_local.cursor()
    sql_insert = "INSERT INTO `macroindex` (`TRADE_DT`, `GDP:mainland`) VALUES ('{DT}', {GDP});"
    sql_update = "UPDATE `macroindex` " \
                 "SET `GDP:mainland`= {GDP} " \
                 "WHERE `TRADE_DT`= '{DT}';"
    count = 0

    for index, srs in df.iterrows():
        # print(index, type(srs))
        # print(index in dateSet)
        args_dict = {
            'GDP': srs.iloc[0],
            'DT': str(index)[:10]
        }

        if index in dateSet:
            cursor.execute(sql_update.format(**args_dict).replace('nan', 'NULL'))
        else:
            cursor.execute(sql_insert.format(**args_dict).replace('nan', 'NULL'))

        conn_local.commit()
        count += 1
        print('Insert %d queries successfully;' % count)