import pymysql
import pandas as pd
import fun_lib as ct
import numpy as np
# import Convert_code as conv

# Test for the encoding
# path_macroindex = 'E:\\1-FuturePlan\\Career\\lcmf\\macro_index\\美国利率.csv'
# path_new = 'E:\\1-FuturePlan\\Career\\lcmf\\macro_index\\美国利率1.csv'
# conv.show_code(path_macroindex)

# conv.convert_code(path_macroindex)
excelFile = u'美国利率.xls'
macro_raw = pd.read_excel(excelFile, index_col=0, parse_dates=['指标名称'])
# the first index is '频率'
macro_raw = macro_raw.drop(macro_raw.index[0])
macro_raw.index = pd.to_datetime(macro_raw.index)
macro_raw = macro_raw.sort_index()


# Preprocessing the macro_raw
#---------------
# Step1: erase the week/season data
# week column '美国:所有联储银行:资产:持有证券:抵押贷款支持债券(MBS)'
# season column '美国:GDP:不变价:环比折年率:季调'
macro_raw.drop('美国:所有联储银行:资产:持有证券:抵押贷款支持债券(MBS)', axis=1, inplace=True)
macro_raw.drop('美国:GDP:不变价:环比折年率:季调', axis=1, inplace=True)

#--------------
# Step2: reduce to monthly data
# generate timeseries beginning at '1997-01-31', with diff Month, and ending at '2019-05-31'
end_month_timeseries = pd.date_range(start='1997-07-31', end='2019-05-31', freq='M')
time_ser = pd.Series(np.random.rand(len(end_month_timeseries)), index=end_month_timeseries)

macro_raw_inter = macro_raw.copy()
macro_raw_inter = macro_raw_inter.fillna(method='bfill')
macro_raw_inter = macro_raw_inter.fillna(method='ffill')

macro_month = macro_raw_inter.copy()
macro_month = macro_month.reindex(time_ser.index)
# macro_month = macro_month.sort_index()

#--------------
# Step3: random generator
# left or right stdev
# Structure of df(parameter):
def random_generator(df):
    df_copy = df.copy()
    for col, series in df.iteritems():
        std = series.std()
        variation_ser_ = pd.Series((2*np.random.rand(len(df))-1)*std, index=df.index)
        df_copy[col] = df_copy[col] + variation_ser_

    return df_copy







#-----------------------------
# select 标普500 globalid = 120000013
conn2 = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='mofang', charset='utf8')
sql_SP500 = "SELECT ra_date, ra_nav FROM ra_index_nav WHERE ra_index_id = '120000013'"
SP500_raw = pd.read_sql(sql=sql_SP500, con=conn2, parse_dates=['ra_date'])
SP500_raw = SP500_raw.set_index("ra_date")
SP500_raw = SP500_raw.sort_index()

# extract end-of-month data before computing variation ratio
SP500_trunc = SP500_raw.reindex(time_ser.index)
SP500_trunc = SP500_trunc.sort_index()

# use 'pct_change' to compute the percentage change
SP500_inc_trunc = SP500_trunc.copy()
SP500_inc_trunc['ra_nav'] = SP500_inc_trunc['ra_nav'].pct_change()
SP500_inc_trunc = SP500_inc_trunc.drop(SP500_inc_trunc.index[0])

# SP500 is stored in natural dates
# SP500_inc_trunc = SP500_inc.copy()
# SP500_inc_trunc = SP500_inc_trunc.reindex(end_month_timeseries)
# SP500_inc_trunc = SP500_inc_trunc.sort_index()






#------------------------------------------
# generate the label dataframe
def binary_filter(x):
    if x.iloc[0] > 0:
        return 1
    else:
        return 0

SP500_label = SP500_inc_trunc.apply(binary_filter, axis=1)






# Input: macro_month
# xgboost module
# xgboost module
import xgboost as xgb

# source code not function
# # temp is used for the amount check
# # split the date into pieces with ratio 5:2:3
# ratio_mark_temp = [round(len(SP500_label)*0.5), round(len(SP500_label)*0.2), round(len(SP500_label)*0.3)]
# ratio_mark = [ratio_mark_temp[0], ratio_mark_temp[0]+ratio_mark_temp[1],
#               ratio_mark_temp[0]+ratio_mark_temp[1]+ratio_mark_temp[2]]
#
# # transform the data into DMatrix type
# dtrain = xgb.DMatrix(macro_month[:ratio_mark[0]], label=SP500_label[:ratio_mark[0]])
#
# dvalid = xgb.DMatrix(macro_month[ratio_mark[0]:ratio_mark[1]], label=SP500_label
#                     [ratio_mark[0]:ratio_mark[1]])
#
# dtest = xgb.DMatrix(macro_month[ratio_mark[1]:ratio_mark[2]], label=SP500_label
#                     [ratio_mark[1]:ratio_mark[2]])
#
#
#
#
#
# # Explanation for parameters:
# # max_depth: maximum depth
# # eta: like learning rate
# # silent: whether output the intermediate result
# # objective: ensure type of objective function
# # nthread: maximum thread to be used
# # eval_metric:
# # use space+\ to mullti-coding
# param = {
#         'max_depth': 2, 'eta': 1, 'silent': 1, 'objective': 'binary:logistic', 'nthread': 4,
#         'eval_metric': 'auc'
#         }
#
# # num_round: total times of training
# num_round = 10
#
# evallist = [(dvalid, 'eval'), (dtrain, 'train')]
#
# bst = xgb.train(param, dtrain, num_round, evallist)
#
#
#
#
# # Test using data from training set
# # Given data, the prediction is stable.
# ypred = bst.predict(dtest)
ratio_mark_temp = [round(len(SP500_label) * 0.5), round(len(SP500_label) * 0.2), round(len(SP500_label) * 0.3)]
ratio_mark = [ratio_mark_temp[0], ratio_mark_temp[0] + ratio_mark_temp[1],
                  ratio_mark_temp[0] + ratio_mark_temp[1] + ratio_mark_temp[2]]

real_test_label = SP500_label[ratio_mark[1]:ratio_mark[2]]

def xgb_pred(macro_month, SP500_label):
    # temp is used for the amount check
    # split the date into pieces with ratio 5:2:3


    # transform the data into DMatrix type
    dtrain = xgb.DMatrix(macro_month[:ratio_mark[0]], label=SP500_label[:ratio_mark[0]])

    dvalid = xgb.DMatrix(macro_month[ratio_mark[0]:ratio_mark[1]], label=SP500_label
    [ratio_mark[0]:ratio_mark[1]])

    dtest = xgb.DMatrix(macro_month[ratio_mark[1]:ratio_mark[2]], label=SP500_label
    [ratio_mark[1]:ratio_mark[2]])

    # Explanation for parameters:
    # max_depth: maximum depth; increasing this value will make the model more complex and more likely to overfit, range: [0,∞]
    # eta: like learning rate, range: [0,1]
    # gamma: Minimum loss reduction required to make a further partition on a leaf node of the tree, range: [0,∞]
    # silent: whether output the intermediate result
    # objective: ensure type of objective function
    # nthread: maximum thread to be used
    # eval_metric:
    # use space+\ to mullti-coding
    param = {
        'max_depth': 2, 'eta': 0.3, 'silent': 0, 'objective': 'binary:logistic', 'nthread': 4,
        'eval_metric': 'auc'
    }

    # num_round: total times of training
    num_round = 100

    evallist = [(dvalid, 'eval'), (dtrain, 'train')]

    bst = xgb.train(param, dtrain, num_round, evallist)

    # Test using data from training set
    # Given data, the prediction is stable.
    ypred = bst.predict(dtest)
    return ypred



pred_df = pd.DataFrame(columns=[i for i in range(ratio_mark_temp[-1]-1)])
# pred_list = []
for i in range(10):
    temp_macro_month = random_generator(macro_month)
    # pred_list.append(xgb_pred(temp_macro_month, SP500_label))
    ser_temp = pd.Series(xgb_pred(temp_macro_month, SP500_label))
    pred_df = pred_df.append(ser_temp, ignore_index=True)




#------------------------------
# plot module
# import matplotlib.pyplot as plt
#
# results = pd.read_csv('10000test.csv', index_col=['index'])
# # column corresponds to the real value: real_test_label, type=DataSeries
# fig = plt.figure(figsize=(20,20))
# ax11 = fig.add_subplot(221)
# ax12 = fig.add_subplot(222)
# ax21 = fig.add_subplot(223)
# ax22 = fig.add_subplot(224)
# ax11.hist(results.iloc[:,0].values,bins=800,color='r',alpha=0.4,edgecolor='b')
# ax12.hist(results.iloc[:,1].values,bins=800,color='r',alpha=0.4,edgecolor='b')
# ax21.hist(results.iloc[:,2].values,bins=800,color='r',alpha=0.4,edgecolor='b')
# ax22.hist(results.iloc[:,3].values,bins=800,color='r',alpha=0.4,edgecolor='b')
