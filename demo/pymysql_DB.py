import pymysql
import pandas as pd
from packages_ct import fun_lib as ct

'''
Please re-check the database should any change of names appears.

Part 1: Read from DB
conn = pymysql.connect(host='IP', user='', passwd='', database='', charset=''utf8)
sql_sentence = ""
pd.read_sql(sql=sql_sentence, con=conn)

'''


# construct a connection to database 'lcmf wind'
# Original URL: https://hdb.mofanglicai.com.cn/index.php?server=4
conn = pymysql.connect(host='192.168.88.17', user='finance', passwd='lk8sge9jcdhw', database='lcmf_wind', charset='utf8')

# data from mc_info
sql_info = "SELECT mc_globalid, mc_name FROM mc_info"
info = pd.read_sql(sql=sql_info, con=conn)




info.set_index(['mc_globalid'], inplace=True)


# there are totally 4 spreadsheet in database 'conn', 4 blocks are as follows:
index_list = []
series_list = []

# data from mc_gold_indicator
sql_gold = "SELECT globalid, mc_gold_date, mc_gold_value FROM mc_gold_indicator"
gold_val = pd.read_sql(sql=sql_gold, con=conn, parse_dates=['mc_gold_date'])

# data from mc_real_estate
sql_real_estate = "SELECT globalid, mc_re_date, mc_re_value FROM mc_real_estate"
real_estate_val = pd.read_sql(sql=sql_real_estate, con=conn, parse_dates=['mc_re_date'])

# data from mc_social_finance
sql_social_finance = 'SELECT globalid, mc_sf_date, mc_sf_value FROM mc_social_finance'
social_finance_val = pd.read_sql(sql=sql_social_finance, con=conn, parse_dates=['mc_sf_date'])

# data from mc_us_indicator
sql_us_indicator = 'SELECT globalid, mc_us_date, mc_us_value FROM mc_us_indicator'
us_indicator_val = pd.read_sql(sql=sql_us_indicator, con=conn, parse_dates=['mc_us_date'])

for spreadsheet in [gold_val, real_estate_val, social_finance_val, us_indicator_val]:
    [L1, L2] = ct.filter_by_id(spreadsheet)
    index_list = index_list + L1
    series_list = series_list + L2



#----------------------------
# Establish a generalizationization table depicting the begin and end of data
index = index_list
columns = ['Begin_date', 'Ending_date', 'Num']
Num = [len(x) for x in series_list]
Begin = [x.index[0] for x in series_list]
Ending = [x.index[-1] for x in series_list]

df_generalization = pd.DataFrame(None, index=index, columns=columns)
df_generalization['Begin_date'] = Begin
df_generalization['Ending_date'] = Ending
df_generalization['Num'] = Num



#-----------------------------
# select 标普500 globalid = 120000013
conn2 = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='mofang', charset='utf8')
sql_SP500 = "SELECT ra_date, ra_nav FROM ra_index_nav WHERE ra_index_id = '120000013'"
SP500_raw = pd.read_sql(sql=sql_SP500, con=conn2, parse_dates=['ra_date'])
SP500_raw = SP500_raw.set_index("ra_date")
SP500_raw = SP500_raw.sort_index()

SP500_inc = SP500_raw.copy()
SP500_inc['ra_nav'] = SP500_inc['ra_nav'].pct_change()
SP500_inc = SP500_inc.drop(SP500_inc.index[0])

sql_trade_dates = "SELECT td_date FROM trade_dates"
trade_dates = pd.read_sql(sql=sql_trade_dates, con=conn2, parse_dates=['td_date'])
trade_dates = trade_dates.set_index('td_date')

# reindex fits well with dataframe index, bad with dataframe values
# 97 to 19
SP500_ind_trunc = SP500_inc.reindex(trade_dates.index)
SP500_ind_trunc = SP500_ind_trunc.sort_index()
SP500_ind_trunc = SP500_ind_trunc[SP500_ind_trunc.notnull()]


#------------------------------
# Current Problem:
# Macro index begins from 2016-05 and SP500 ends at 2017-07, which implies that there are only
# 14, 14, 14 available data.
#------------------------------



# # xgboost module
# import xgboost as xgb
#
#
# # split the dataset into 3 parts: train, validation and test
# # to be continued ...
#
#
# # transform the data into DMatrix type
# dtrain = xgb.DMatrix(, )
# dtest = xgb.DMatrix(, )
#
# # Explanation for parameters:
# # max_depth: maximum depth
# # eta: like learning rate
# # silent: whether output the intermediate result
# # objective: ensure type of objective function
# # nthread: maximum thread to be used
# # eval_metric:
# param = {'max_depth':2, 'eta':1, 'silent':1, 'objective':'binary:logistic', 'nthread':4, ...
# 'eval_metric':'auc'}
#
# # num_round: total times of training
# num_round = 10
#
# evallist = [(dtest, 'eval'), (dtrain, 'train')]
#
# bst = xgb.train(param, dtrain, num_round, evallist)
#
#
#
#
# # Test using data from training set
#
# # L = df_core.iloc[1555:1565]
# # L_1 = xgb.DMatrix(L)
# # ypred = bst.predict(L_1)
# # df5.iloc[1555:1565]

