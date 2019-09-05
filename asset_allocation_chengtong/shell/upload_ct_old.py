import pymysql
import pandas as pd
from ipdb import set_trace
from dateutil.parser import parse
from datetime import timedelta


# 建立数据库连接
def get_connection(db_name):
    conn = pymysql.connect(host='hpc', user='root', passwd='Mofang123', db=db_name, charset='gbk')
    cursor1 = conn.cursor()
    return conn, cursor1

# 拆分日期和中文描述混合的标号
def split_ct(x):
    L = x.iloc[0].split()
    x['date'] = parse(L[0])
    x['statistics_type'] = L[1]
    return x

# 截取前num位数据
def round_col(df, num):
    for columns in df.keys():
        df[columns] = df[columns].apply(lambda x: round(x,num))
    return df

# 生成sql语句中VALUES后面的部分
def value_sql(df):
    str_value_sql = ""
    for key in list(df.index):
        L = df.loc[key].values # array
        temp = "('" + str(key) + "', "
        for item in L:
            if str(item) == "nan":
                temp = temp + "'', "
            else:
                temp = temp + "'" + str(item) + "', "
        temp = temp[:-2] + ")"
        str_value_sql = str_value_sql + temp + ", "
    return str_value_sql[:-2]





def insertData_fundpool(db_name, table_name):
    conn, cursor = get_connection(db_name)
    #filename = '基金池收益排名.csv'
    #df = pd.read_csv(filename, index_col=0, encoding='gbk')
    filename = '副本每日数据报告_20190612(1).xlsx'
    df = pd.read_excel(filename, sheetname='基金池', index_col=0, encoding='gbk')
    df = df.reset_index()
    df['date'] = pd.Series(None)
    df['statistics_type'] = pd.Series(None)


    df = df.apply(split_ct, axis=1)
    df.pop('index')
    order = list(df.columns)[-2:] + list(df.columns)[:6]
    df = df[order]

    df.iloc[:,[2,4,6]] = round_col(df.iloc[:,[2,4,6]], 6)



    length = len(df.columns)
    counts = 0
    for each in df.values:
        sql = 'INSERT INTO ' + table_name + ' VALUES ('

        for i, n in enumerate(each):
            if i==length-1:
                if str(n) == 'nan':
                    sql = sql + '"' + '");'
                else:
                    sql = sql + '"' + str(n) + '");'
            else:
                if str(n) == 'nan':
                    sql = sql + '"' + '", '
                else:
                    sql = sql + '"' + str(n) + '", '
        print(sql)
        cursor.execute(sql)
        conn.commit()
        counts += 1
        print('成功添加了'+str(counts)+'条数据')
    return conn, cursor




def insertData_smartrisk(db_name, table_name):
    conn, cursor = get_connection(db_name)
    filename = '副本每日数据报告_20190612(1).xlsx'
    df = pd.read_excel(filename, sheetname='智能组合', index_col=0, encoding='gbk', skiprows=9)
    df = df.reset_index()
    df = df.iloc[:6,:]

    df['date'] = pd.Series(None)
    df['statistics_type'] = pd.Series(None)


    df = df.apply(split_ct, axis=1)
    df.pop('index')

    df.iloc[:,:20] = round_col(df.iloc[:,:20], 6)



    length = len(df.columns)
    counts = 0
    for each in df.values:
        sql = 'INSERT INTO ' + table_name + ' VALUES ('

        for i, n in enumerate(each):
            if i==length-1:
                if str(n) == 'nan':
                    sql = sql + '"' + '");'
                else:
                    sql = sql + '"' + str(n) + '");'
            else:
                if str(n) == 'nan':
                    sql = sql + '"' + '", '
                else:
                    sql = sql + '"' + str(n) + '", '
        print(sql)
        cursor.execute(sql)
        conn.commit()
        counts += 1
        print('成功添加了'+str(counts)+'条数据')
    return conn, cursor





def insertData_macroopinion(db_name, table_name):
    conn, cursor = get_connection(db_name)
    filename = '副本每日数据报告_20190612(1).xlsx'
    df = pd.read_excel(filename, sheetname='宏观观点', index_col=0, encoding='gbk', parse_dates=True)
    df = df.drop(list(df.columns)[-8:-1], axis=1)
    df.columns = df.iloc[0,:].values
    df = df.drop(df.index[0])
    df = df.reset_index()

    df.iloc[:,-8:-1] = round_col(df.iloc[:,-8:-1],6) 

    length = len(df.columns)
    counts = 0
    for each in df.values:
        sql = 'INSERT INTO ' + table_name + ' VALUES ('

        for i, n in enumerate(each):
            if i==length-1:
                if str(n) == 'nan':
                    sql = sql + '"' + '");'
                else:
                    sql = sql + '"' + str(n) + '");'
            else:
                if str(n) == 'nan':
                    sql = sql + '"' + '", '
                else:
                    sql = sql + '"' + str(n) + '", '

        print(sql)
        cursor.execute(sql)
        conn.commit()
        counts += 1
        print('成功添加了'+str(counts)+'条数据')
    return conn, cursor


#def insertData_moderate(db_name_out, table_name_out, db_name_in, table_name_in, ra_portfolio_id, ra_type):
#    conn_out, cursor_out = get_connection(db_name_out)
#    # ra_type == 8: 有费率                   ra_type == 9: 无费率
#    sql_out = "SELECT ra_date, ra_nav FROM " + table_name_out + " WHERE ra_portfolio_id='" + ra_portfolio_id + "' AND ra_type='" + str(ra_type) + "';"
#    df = pd.read_sql(sql=sql_out, con=conn_out, parse_dates=['ra_date'])
#    df = df.set_index('ra_date')
#    TimeSeries = df['ra_nav']
#    df_res = backroll(TimeSeries)
#    df_res = df_res.sort_index(ascending=True)
#
#    return df_res


def insertData_moderate(db_name_out, table_name_out, db_name_in, table_name_in, ra_portfolio_id, ra_type):
    conn_out, cursor_out = get_connection(db_name_out)
    # ra_type == 8: 有费率                   ra_type == 9: 无费率
    sql_out = "SELECT ra_date, ra_nav FROM " + table_name_out + " WHERE ra_portfolio_id='" + ra_portfolio_id + "' AND ra_type='" + str(ra_type) + "';"
    df = pd.read_sql(sql=sql_out, con=conn_out, parse_dates=['ra_date'])
    df = df.set_index('ra_date')
    TimeSeries = df['ra_nav']
    df_res = backroll(TimeSeries)
    df_res = df_res.sort_index(ascending=True)
    df_res['当日点评'] = pd.Series(None) 

    df_original = pd.read_excel('副本每日数据报告_20190612(1).xlsx', sheetname='稳健组合', index_col=0, parse_dates=['稳健组合收益'])
    
    df_total = pd.concat([df_original, df_res])
    df_total = df_total.sort_index(ascending=True)

    conn_in, cursor_in = get_connection(db_name_in)

    # 截取表的所有列名，拼接成字符串，为sql语句做准备
    df_columns = pd.read_sql(sql="SELECT * FROM " + table_name_in + ";", con=conn_in)
    columns = list(df_columns.columns)[1:-2]
    columns_sql_part = "("
    for item in columns:
        if item != columns[-1]:
            columns_sql_part = columns_sql_part + "`" + item + "`, "
        else:
            columns_sql_part = columns_sql_part +  "`" + item + "`)"

    sql = "INSERT INTO " + table_name_in + " " + columns_sql_part + " VALUES "
    value_sql_part = value_sql(df_total)
    sql = sql + value_sql_part + ";" 

    cursor_in.execute(sql)
    conn_in.commit()

    return


def backroll(TS):
    # 判断是否是闰年 leap year
    year = timedelta(365)
    month_1 = timedelta(30)
    month_2 = timedelta(90)
    half_year = timedelta(180)
    # 时间倒序排列，因为可求项位于时间序列的末端
    ts = TS.sort_index(ascending=False)

    df = pd.DataFrame(None, columns=['过去一月','过去三月','过去半年','过去一年'])
    query = dict()
    for date, nav in ts.iteritems():
        if  (date - year in ts.index):
            query['过去一月'] = (ts[date] - ts[date-month_1])/ts[date-month_1]
            query['过去三月'] = (ts[date] - ts[date-month_2])/ts[date-month_2]
            query['过去半年'] = (ts[date] - ts[date-half_year])/ts[date-half_year]
            query['过去一年'] = (ts[date] - ts[date-year])/ts[date-year]
            temp = pd.DataFrame(query, index=[date])
            df = pd.concat([df,temp])
        else:
        # 一旦当前日期没有过去一年的涨跌幅，那么下面的所有数据都没有，循环需要停止；
            break
    return df







db_name_in = 'asset_allocation'
db_name_out = 'asset_allocation'


table_name_out = 'ra_portfolio_nav'
table_name_in = 'moderate_info_pdate'

ra_portfolio_id = 'PO.CB0100'
ra_type = 8

#table_name_pool = 'fund_pool_info_pdate (fp_date, fp_statistics_type, fp_average_return_bond, fp_rank_bond, fp_average_return_stock, fp_rank_stock, fp_average_return_currency, fp_rank_currency)'
#table_name_risk = 'smart_risk_info_pdate (sir_risk1, sir_risk2, sir_risk3, sir_risk4, sir_risk5, sir_risk6, sir_risk7, sir_risk8, sir_risk9, sir_risk10, sir_risk1_standard, sir_risk2_standard, sir_risk3_standard, sir_risk4_standard, sir_risk5_standard, sir_risk6_standard, sir_risk7_standard, sir_risk8_standard, sir_risk9_standard, sir_risk10_standard, sir_date, sir_statistics_type)'
#table_name_macro = 'opinion_info_pdate (`oi_date`, `oi_opinion_large_cap_stock`, `oi_opinion_small_cap_stock`, `oi_opinion_US_stock`, `oi_opinion_HK_stock`, `oi_opinion_gold`, `oi_opinion_rate_bond`, `oi_opinion_debenture_bond`, `oi_opinion_oril`, `oi_return_CSI300`, `oi_return_IC500`, `oi_return_S&P500`, `oi_return_HSI`, `oi_return_SQau`, `oi_return_CSI_TB`, `oi_return_CSI_DB`, `oi_comment`)'

#set_trace()
#insertData_fundpool(db_name, table_name_pool)
#insertData_smartrisk(db_name, table_name_risk)
#insertData_macroopinion(db_name, table_name_macro)
insertData_moderate(db_name_out, table_name_out, db_name_in, table_name_in, ra_portfolio_id, ra_type)
#print(df)
