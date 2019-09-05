import pymysql
import pandas as pd


def hello():
    print('Hello World!')


# return the beginning date of a dataframe
def date_begin(df):
    # date is stored in the first column
    df_1 = df.sort_values(by=[df.columns[0]])
    index = df_1.columns[0]
    return str(df_1[index].iloc[0])


# return the ending date of a dataframe
def date_end(df):
    # date is stored in the first column
    df_1 = df.sort_values(by=[df.columns[0]], ascending=False)
    return str(df_1[df_1.columns[0]].iloc[0])


# input: a dataframe with columns = ['globalid','date','value'] and default index
# output: a list of dataseries with index=date and column=globalid
def filter_by_id(df):
    df_copy = df.copy()
    df_copy.set_index(['globalid'], inplace=True)

    index_list = list(df_copy.index.unique())

    ds_container = []
    for index in index_list:
        temp = df_copy.loc[index]
        ds = pd.Series(temp.iloc[:,1].values, index=temp.iloc[:,0])
        ds.sort_index(inplace=True)
        ds_container.append(ds)

    return index_list, ds_container

# input: a time_series
# output: a new time_series with no missing value and sorted date index
def ts_preprocess(ts):
    tmp = ts.sort_index()
    tmp = tmp.fillna(method='ffill')
    tmp = tmp.fillna(method='bfill')
    return tmp

def list_ts_preprocess(list_ts):
    list_tmp = [ts_preprocess(ts) for ts in list_ts]

    common_index = list_tmp[0].index & list_tmp[1].index
    if len(list_tmp) > 2:
        for ts in list_tmp[2:]:
            common_index = common_index & ts.index

    list_res = [ts.reindex(common_index) for ts in list_tmp]

    return list_res










# def construct_date_id(cursor):
#         import pandas as pd
#
#         sql1 = "SELECT ra_index_id, ra_date FROM ra_index_nav WHERE ra_index_id REGEXP '01$|02$|13$|14$|15$'"
#         raw_index_tuple = FetchTuple(cursor, sql1)
#         raw_index_tuple = list(raw_index_tuple)
#
#
#
#         sql2 = "SELECT ra_inc FROM ra_index_nav WHERE ra_index_id REGEXP '01$|02$|13$|14$|15$'"
#         raw_value_tuple = FetchTuple(cursor, sql2)
#         container = []
#
#         for item in raw_value_tuple:
#                 container.append(item[0])
#
#
#         ds = pd.Series(container, index = pd.MultiIndex.from_tuples(raw_index_tuple) )
#         return ds
#
#
def filter_trade(cursor, df):
        # import time, pandas as pd

        sql = "SELECT td_date FROM trade_dates"
        raw_date_tuple = fetchtuple(cursor, sql)

        container = []
        for item in raw_date_tuple:
                container.append(item[0].strftime("%Y-%m-%d"))

        # container2_df = []
        # for str_date in container:
        #         container2_df.append(df[str_date:str_date])
        #         df = df.set_value(df_13.index[i],'mark', 0)
        df = df.reindex(container)
        return df

# # input two dataframe and a model from xgb
# def confusionmatrix(df_feature, df_label, bst):
#         import xgboost as xgb
# ypred = bst.predict(xgb.DMatrix(df_feature))
#
# ypred = list(ypred)
#
# for i in range(len(ypred)):
#         if ypred[i]>1e-03:
#                 ypred[i] = 1
#         else:
#                 ypred[i] = 0
#
# yreal = list(df_label['mark'])
#
# CM = [0,0,0,0]
#
# for i in range(len(ypred)):
#         if yreal[i] == 1:
#                 if ypred[i] == 1:
#                         CM[0] = CM[0] +1
#                 else:
#                         CM[1] = CM[1] +1
#         else:
#                 if ypred[i] == 1:
#                         CM[2] = CM[2] +1
#                 else:
#                         CM[3] = CM[3] +1
# return CM
#
#
#
#
# # add mark according to inc and filter NaN and 0E-08
# def addmark(df):
#         import pandas as pd
#         df = df[df>1e-05]
#
#         df = pd.DataFrame([df,pd.Series([])], index=['ra_inc','mark'])
#         df = df.T
#
#         for i in range(0,len(df)):
#                 if df.iat[i,0]>0:
#                         df = df.set_value(df.index[i],'mark', 1)
#                 else:
#                         df = df.set_value(df.index[i],'mark', 0)
#
#         df = df[df['ra_inc'].notnull()]
#         df = df.sort_index(ascending=True)
#
#         return df

# def expand(df, num):
#         import pandas as pd
#         if num > len(df)-1:
#                 print('num is so large!')
#                 return
#
#         ds_container = []
#         for i in range(num):
#                 temp = i+1
#                 df['ra_inc'].iloc[:len(df)]
#                 ds_container.append()



