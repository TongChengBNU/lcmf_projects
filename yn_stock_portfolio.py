'''
此代码为实验性代码，上线时需修改pymysql相关代码
Created on: Mar. 11, 2019
Modified on: Jun. 25, 2019
Author: Shixun Su, Boyang Zhou, Ning Yang
Contact: sushixun@licaimofang.com
'''

import logging
from functools import partial
import multiprocessing
import numpy as np
import pandas as pd
import scipy.cluster.hierarchy as hierarchy
import statsmodels.api as sm
import cvxopt
import math
import hashlib
import re
import copy
import pymysql
import cvxpy as cp
from ipdb import set_trace

from sixpence.db import wind_asharecalendar
from sixpence.db import wind_aindexeodprices, wind_aindexmembers
from sixpence.db import wind_asharedescription, wind_ashareswindustriesclass, wind_ashareeodprices
from sixpence.db import wind_asharecapitalization, wind_asharefreefloat, wind_ashareeodderivativeindicator, \
    wind_asharedividend
from sixpence.db import wind_ashareipo, wind_asharestockswap
from sixpence.db import factor_financial_statement
from sixpence.db import asset_sp_stock_portfolio, asset_sp_stock_portfolio_nav, asset_sp_stock_portfolio_pos
from sixpence.db import factor_sf_stock_factor_exposure
from sixpence.multiprocessing_tools import multiprocessing_map
from sixpence.trade_date import *
from sixpence import calc_covariance
from sixpence import calc_descriptor
from sixpence import statistic_tools_multifactor
from sixpence.stock_portfolio import *
import xgboost as xgb

logger = logging.getLogger(__name__)


class StockPortfolioYN(StockPortfolio):

    _kwargs_list = []

    def __init__(self, index_id, reindex, look_back, **kwargs):
        super(StockPortfolioYN, self).__init__(index_id, reindex, look_back, **kwargs)
        self.style_factor = ['value', 'earnings', 'short_term_growth', 'estimate', 'linear_size', 'beta', 'liquidity', 'reverse']
        self.alpha_factor_p = ['value', 'earnings', 'short_term_growth', 'estimate']
        self.alpha_factor_n = ['liquidity', 'reverse']
        self.risk_factor = ['linear_size', 'beta']
        self.observe_period = 242 * 5
        self._df_stock_factor_exposure = factor_sf_stock_factor_exposure.load_a_stock_factor_exposure(stock_ids=self.stock_pool.index, style_factors=self.style_factor, table_name='sf_stock_factor_exposure_z_score_0903_neutral')
        self.tvalues_threshold_p = 2.0
        self.tvalues_threshold_n = 2.0
        self.max_percentage = 0.01
        self.trading_frequency = 10
        self.trading_fee = 0.006 * 242 / self.trading_frequency  # 双边千6

        conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='multi_factor', charset='utf8')
        sql_t = 'select * from ft_factor_test_new2'
        df_FactorReturn = pd.read_sql(con=conn, sql=sql_t, parse_dates=['trade_date']).fillna(0.0)
        conn.close()

        df_FactorReturn = df_FactorReturn.loc[~df_FactorReturn.target_exposure.isin([-1.4, 1.4])]
        self.target_exposure = df_FactorReturn.target_exposure.unique()
        self.exposure_target_interval = 0.4
        self.max_factor_exposure = 1.2

        dict_FactorReturn = dict()
        for i_target_exposure in self.target_exposure:
            df_FactorReturn_t = df_FactorReturn.loc[df_FactorReturn.target_exposure == i_target_exposure, ['trade_date'] + self.style_factor].set_index('trade_date').sort_index() / self.trading_frequency
            dict_FactorReturn[i_target_exposure] = df_FactorReturn_t.copy()
        self._dict_FactorReturn = dict_FactorReturn.copy()

    def _calc_stock_pos(self, trade_date):
        stock_ids = self._load_stock_ids(trade_date)

        trade_dates_total = self.trade_dates_total.copy()
        factor_exposure_date = trade_dates_total[trade_dates_total < trade_date].sort_values()[-1]

        df_FactorExposure_t = self._df_stock_factor_exposure.loc[factor_exposure_date].copy()
        df_FactorExposure_t = df_FactorExposure_t.loc[df_FactorExposure_t.index.isin(stock_ids)]
        df_FactorExposure_t[self.style_factor] = df_FactorExposure_t[self.style_factor].fillna(df_FactorExposure_t[self.style_factor].mean())
        df_FactorExposure_t['weight'] = df_FactorExposure_t['weight'] / df_FactorExposure_t['weight'].sum()
        ser_IndexFactorExposure = pd.Series(data=np.dot(df_FactorExposure_t[self.style_factor].T, df_FactorExposure_t['weight']), index=[self.style_factor])

        ser_EstStockReturn = pd.Series(data=0.0, index=df_FactorExposure_t.index)
        for j_target_exposure in self.target_exposure:
            if j_target_exposure == self.target_exposure.min():
                max_exposure = self.target_exposure.min() + self.exposure_target_interval / 2
                min_exposure = -np.inf
            elif j_target_exposure == self.target_exposure.max():
                max_exposure = np.inf
                min_exposure = self.target_exposure.max() - self.exposure_target_interval / 2
            else:
                max_exposure = j_target_exposure + self.exposure_target_interval / 2
                min_exposure = j_target_exposure - self.exposure_target_interval / 2

            df_FactorExposure_t2 = df_FactorExposure_t[self.style_factor].applymap(lambda x: self.calc_stock_return(x, upper_limit=max_exposure, lower_limit=min_exposure))
            df_FactorReturn_t = self._dict_FactorReturn[j_target_exposure].loc[:factor_exposure_date].iloc[-self.observe_period:].copy()
            # t_value notice: 自相关导致该t值会显著高估，后期应进行修改
            ser_t_values_t = df_FactorReturn_t.mean() * np.sqrt(df_FactorReturn_t.shape[0]) / df_FactorReturn_t.std()
            ser_FactorReturn_t = df_FactorReturn_t.mean() * 242
            # adjust factor return
            ser_AdjFactorReturn_t = pd.Series(data=0.0, index=self.style_factor)
            for k_style_factor in self.style_factor:
                if j_target_exposure >= 0.0:
                    if ser_t_values_t[k_style_factor] > self.tvalues_threshold_p and k_style_factor in self.alpha_factor_p:
                        ser_AdjFactorReturn_t[k_style_factor] = ser_FactorReturn_t[k_style_factor]
                    elif ser_t_values_t[k_style_factor] < - self.tvalues_threshold_n and k_style_factor in self.alpha_factor_n:
                        ser_AdjFactorReturn_t[k_style_factor] = ser_FactorReturn_t[k_style_factor]
                    else:
                        pass
                else:  # j_target_exposure < 0.0:
                    if ser_t_values_t[k_style_factor] < - self.tvalues_threshold_n and k_style_factor in self.alpha_factor_p:
                        ser_AdjFactorReturn_t[k_style_factor] = ser_FactorReturn_t[k_style_factor]
                    elif ser_t_values_t[k_style_factor] > self.tvalues_threshold_p and k_style_factor in self.alpha_factor_n:
                        ser_AdjFactorReturn_t[k_style_factor] = ser_FactorReturn_t[k_style_factor]
                    else:
                        pass
            #
            ser_EstStockReturn_t = pd.Series(data=np.dot(df_FactorExposure_t2.values, ser_AdjFactorReturn_t.values), index=df_FactorExposure_t2.index)
            ser_EstStockReturn = ser_EstStockReturn + ser_EstStockReturn_t
        # considering fee
        if trade_date == self.reindex[0]:
             ser_AddStockReturn = self._df_stock_pos.loc[trade_date].reindex(ser_EstStockReturn.index).fillna(0.0)
        else:
            last_trade_date = self.reindex[self.reindex < trade_date][-1]
            ser_AddStockReturn = self._df_stock_pos.loc[last_trade_date].reindex(ser_EstStockReturn.index).fillna(0.0)
        ser_AddStockReturn[ser_AddStockReturn >= self.max_percentage/10] = self.trading_fee
        ser_EstStockReturn = ser_EstStockReturn + ser_AddStockReturn

        n = df_FactorExposure_t.shape[0]
        x = cp.Variable(n)

        G_RiskFactor = df_FactorExposure_t[self.risk_factor].values.T
        h_RiskFactor = ser_IndexFactorExposure.loc[self.risk_factor].values
        obj = cp.Minimize(-x @ ser_EstStockReturn.values)
        G_AlphaFactorP = df_FactorExposure_t[self.alpha_factor_p].values.T
        h_AlphaFactorP = np.ones(len(self.alpha_factor_p)) * self.max_factor_exposure
        G_AlphaFactorN = df_FactorExposure_t[self.alpha_factor_n].values.T
        h_AlphaFactorN = -np.ones(len(self.alpha_factor_n)) * self.max_factor_exposure
        con = [
            x >= 0, x <= self.max_percentage, x @ np.ones(n) == 1.0, G_RiskFactor @ x == h_RiskFactor,
            G_AlphaFactorP @ x <= h_AlphaFactorP, G_AlphaFactorP @ x >= 0.0,
            G_AlphaFactorN @ x >= h_AlphaFactorN, G_AlphaFactorN @ x <= 0.0,
        ]

        prob = cp.Problem(obj, con)
        if prob.solve() in [np.inf, -np.inf]:
            print(trade_date, 'error')
        else:
            print(trade_date, 'success')

        df_FactorExposure_t['opt_weight'] = x.value
        df_FactorExposure_t.loc[df_FactorExposure_t.opt_weight < self.max_percentage/10, 'opt_weight'] = 0.0
        df_FactorExposure_t['opt_weight'] = df_FactorExposure_t['opt_weight'] / df_FactorExposure_t['opt_weight'].sum()

        stock_pos = df_FactorExposure_t.opt_weight
        stock_pos.name = trade_date
        return stock_pos

    def calc_stock_return(self, x, upper_limit, lower_limit):
        if lower_limit <= x < upper_limit:
            return 1.0
        else:
            return 0.0


if __name__ == '__main__':
    from sixpence.yn_stock_portfolio import StockPortfolioYN
    import pandas as pd
    from sixpence.db import wind_asharecalendar
    index_id = '000905.SH'; begin_date = '2018-01-01'; end_date = '2019-03-29'; look_back = 244
    reindex = wind_asharecalendar.load_a_trade_date(begin_date=begin_date, end_date=end_date, period='half_month')
    stock_portfolio = StockPortfolioYN(index_id, reindex, look_back)
    nav = stock_portfolio.calc_portfolio_nav(considering_status=True, considering_fee=True)

