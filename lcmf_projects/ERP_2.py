#coding=utf8

import pymysql
import pandas as pd
import numpy as np
from dateutil.parser import parse
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import matplotlib as mpl
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import statsmodels.api as sm
from time import time

plt.rcParams['font.sans-serif']=['Simhei'] #解决中文显示问题，目前只知道黑体可行
plt.rcParams['axes.unicode_minus']=False #解决负数坐标显示问题


class plot_CT:

    def __init__(self):
        self.plt_figsize = {
                        'Low': (8,6),
                        'Middle': (10.24, 7.68),
                        'High': (19.20, 10.8)
                      }
        self.legend_loc = {
                        'BestPossible': 0,
                        'UpperRight': 1,
                        'UpperLeft': 2,
                        'LowerLeft': 3,
                        'LowerRight': 4,
                        'Right': 5,
                        'CenterLeft': 6,
                        'CenterRight': 7,
                        'LowerCenter': 8,
                        'UpperCenter': 9,
                        'Center': 10
                        }
        self.color = {
            'Red': 'r',
            'Green': 'g',
            'Blue': 'b',
        }
        self.line_style = {
            'Dot': 'o'
        }



    def basic_line_single(self, data: pd.DataFrame, grid=False, xlabel='', ylabel='', title='', xlim=None, ylim=None):
        plt.figure(figsize=self.plt_figsize['Middle'])
        for columnName in data:
            column = data[columnName]
            plt.plot((column.index.to_pydatetime()), column.values, lw=1.5, label=column.name)
        plt.grid(grid)
        plt.axis('tight')

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(loc=self.legend_loc['UpperRight'])

        plt.title(title)
        if xlim is not None:
            plt.xlim(xlim)
        if ylim is not None:
            plt.ylim(ylim)

        plt.show()


    def secondary_line_single(self, data1: pd.DataFrame, data2: pd.DataFrame, grid=False, xlabel='', y1label=''
                              , y2label='', title='', xlim=None, ylim=None):
        fig, ax1 = plt.subplots(figsize=self.plt_figsize['Middle'])
        plt.sca(ax=ax1)
        for columnName in data1:
            column = data1[columnName]
            plt.plot((column.index.to_pydatetime()), column.values, lw=1.5, label=column.name)
        plt.grid(grid)
        plt.axis('tight')

        plt.xlabel(xlabel)
        plt.ylabel(y1label)
        plt.legend(loc=self.legend_loc['UpperRight'])
        plt.title(title)

        if xlim is not None:
            plt.xlim(xlim)
        if ylim is not None:
            plt.ylim(ylim)

        ax2 = ax1.twinx()
        plt.sca(ax=ax2)
        for columnName in data2:
            column = data2[columnName]
            plt.plot((column.index.to_pydatetime()), column.values, lw=2, label=column.name)
        plt.legend(loc=self.legend_loc['LowerCenter'])
        plt.ylabel(y2label)

        plt.show()

    def basic_bar_single(self, data: pd.DataFrame, grid=False, xlabel='', ylabel='', title='', xlim=None, ylim=None):
        plt.figure(figsize=self.plt_figsize['Middle'])
        if len(data.columns) > 1:
            print('Two much columns.')
            return
        data = data.iloc[:,0]
        plt.bar(data.index, data.values, width=0.5, color='r', label=data.name)
        plt.grid(grid)
        plt.axis('tight')

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(loc=self.legend_loc['UpperRight'])

        plt.title(title)
        if xlim is not None:
            plt.xlim(xlim)
        if ylim is not None:
            plt.ylim(ylim)

        plt.show()

    def basic_line_two(self, data1: pd.DataFrame, data2: pd.DataFrame, grid=False, xlabel='', ylabel='', title='', xlim=None, ylim=None, size=(1,1)):
        row = size[0]
        column = size[1]
        plt.figure(figsize=self.plt_figsize['Middle'])
        plt.subplot(211)
        for columnName in data1:
            column = data1[columnName]
            plt.plot((column.index.to_pydatetime()), column.values, lw=1.5, label=column.name)
        plt.grid(grid)
        plt.axis('tight')

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(loc=self.legend_loc['UpperRight'])

        plt.title(title)
        if xlim is not None:
            plt.xlim(xlim)
        if ylim is not None:
            plt.ylim(ylim)

        plt.subplot(212)
        for columnName in data2:
            column = data2[columnName]
            plt.plot((column.index.to_pydatetime()), column.values, lw=1.5, label=column.name)
        plt.grid(grid)
        plt.axis('tight')

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(loc=self.legend_loc['UpperRight'])

        plt.title(title)
        if xlim is not None:
            plt.xlim(xlim)
        if ylim is not None:
            plt.ylim(ylim)

        plt.show()





def DBconnection(DBname):
    '''
    Choose 'DBname' between: wind, mofang
    '''
    # host = '192.168.88.11', user = 'public', passwd = 'h76zyeTfVqAehr5J', database = 'wind', charset = 'utf8')
    args_db_wind = {
        'host': '192.168.88.11',
        'user': 'public',
        'passwd': 'h76zyeTfVqAehr5J',
        'database': 'wind',
        'charset': 'utf8'
    }

    args_db_ct = {
        'host': 'localhost',
        'user': 'root',
        'passwd': 'chengtong123',
        'database': 'finance',
        'charset': 'utf8'
    }

    args_db_root = {
        'host':'192.168.88.254',
        'user':'root',
        'passwd':'Mofang123',
        'database':'mofang',
        'charset': 'utf8'
    }

    if DBname == 'wind':
        # conn = pymysql.connect(host='192.168.88.11', user='public', passwd='h76zyeTfVqAehr5J', database='wind', charset='utf8')
        conn = pymysql.connect(**args_db_wind)
    elif DBname == 'ct':
        conn = pymysql.connect(**args_db_ct)
    elif DBname == 'root':
        # conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='mofang', charset='utf8')
        conn = pymysql.connect(**args_db_root)
    else:
        print('Please specify \'DBname\'. ')
        return

    return conn
def datetimeProcess(date):
    if type(date) == str:
        return parse(date)
    else:
        return date
def timestampTostr(date):
    return (str(date)[:10]).replace('-', '')


class base:

    def __init__(self):
        self.conn_wind = DBconnection(DBname='wind')
        self.conn_ct = DBconnection(DBname='ct')
        self.plotHandle = plot_CT()
        self.bond_code_dic = {
            '中债-0-3个月国债指数': 'CBA07301.CS',
            '中债-企业债总指数': 'CBA02001.CS',
            '中债-10年期国债指数': 'CBA04501.CS',
            '中债-3-5年期国债指数': 'CBA04601.CS',
            '中债-企业债AAA指数': 'CBA04201.CS',
            '中债-信用债总指数': 'CBA02701.CS',
        }
        self.name = 'ChengTong'

    # @property
    # def get_conn(self):
    #     return self.__conn_wind
    #
    # @property
    # def get_name(self):
    #     return self.__name

    def hushen300(self):
        '''
        hushen300_df: ['DIVIDEND_YIELD', 'PE_TTM', 'MV_FLOAT', 'DIVIDEND_TOTAL', 'DIVIDEND_YIELD_PAYMENT_RATE']
        hushen300quan_df: ['S_DQ_CLOSE', 'FUTURE_1_Y_RETURN']

        :return:
        '''
        sqlMy = "SELECT TRADE_DT, DIVIDEND_YIELD, PE_TTM, MV_FLOAT, EST_PE_Y1, TOT_SHR_FLOAT FROM aindexvaluation WHERE S_INFO_WINDCODE = '000300.SH';"
        hushen300_df = pd.read_sql(sql=sqlMy, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        hushen300_df.sort_index(inplace=True)

        hushen300_df['DIVIDEND_TOTAL'] = hushen300_df['DIVIDEND_YIELD'] * hushen300_df['MV_FLOAT'] / 100
        hushen300_df['DIVIDEND_YIELD_PAYMENT_RATE'] = hushen300_df['DIVIDEND_YIELD'] * hushen300_df['PE_TTM'] / 100

        sqlMy = "SELECT TRADE_DT, S_DQ_CLOSE FROM `aindexeodprices` WHERE `S_INFO_WINDCODE` = 'H00300.CSI';"
        hushen300quan_df = pd.read_sql(sql=sqlMy, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        hushen300quan_df.sort_index(inplace=True)

        # 沪深 300 未来一年收益
        hushen300quan_df['FUTURE_1_Y_RETURN'] = pd.Series()
        for index in hushen300quan_df.index:
            future = index + timedelta(days=250)
            if future in hushen300quan_df.index:
                hushen300quan_df.loc[index, 'FUTURE_1_Y_RETURN'] = hushen300quan_df.loc[future, 'S_DQ_CLOSE'] / \
                                                                   hushen300quan_df.loc[index, 'S_DQ_CLOSE'] - 1
        hushen300quan_df['FUTURE_1_Y_RETURN'].fillna(method='ffill', inplace=True)

        return hushen300_df, hushen300quan_df

        # 10 CBA04501    3-5 CBA04601

    def bond(self, description=('中债-3-5年期国债指数', '中债-10年期国债指数')):
        '''
        bond3_5_df: YTM
        bond10_df: YTM
        :return:
        '''
        # '中债-10年期国债指数': 'CBA04501.CS',
        #             '中债-3-5年期国债指数': 'CBA04601.CS',

        res = dict()
        for index, name in enumerate(description):
            if name in self.bond_code_dic.keys():
                print(str(index) + ': ' + name + ' is included in code set.')
                sqlMy = "SELECT TRADE_DT, AVG_CASH_YTM FROM cbondindexeodcnbd WHERE S_INFO_WINDCODE = '" + \
                        self.bond_code_dic[name] + "' ORDER BY `TRADE_DT` ASC;"
                bond_df = pd.read_sql(sql=sqlMy, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])

                res[name] = bond_df

        return res

        # sqlMy1 = "SELECT TRADE_DT, AVG_CASH_YTM FROM cbondindexeodcnbd WHERE S_INFO_WINDCODE = 'CBA04501.CS';"
        # sqlMy2 = "SELECT TRADE_DT, AVG_CASH_YTM FROM cbondindexeodcnbd WHERE S_INFO_WINDCODE = 'CBA04601.CS';"
        # bond10_df = pd.read_sql(sql=sqlMy1, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        # bond3_5_df = pd.read_sql(sql=sqlMy2, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        # bond10_df.sort_index(inplace=True)
        # bond3_5_df.sort_index(inplace=True)
        # return bond3_5_df, bond10_df

    def inflation(self):
        '''

        :return: CPI_PPI_df, GDPpingjian_df
        '''
        # path = "E:/1-Career/lcmf/Projects/ERP"
        # CPI = pd.read_excel(path + "/CPI.xls", parse_dates=['date'])
        # PPI = pd.read_excel(path + "/PPI.xls", parse_dates=['date'])
        #
        # dfCore = pd.merge(left=CPI, right=PPI, how='inner', left_on='date', right_on='date')
        # dfCore.set_index('date', inplace=True)
        # dfCore['Inflation'] = dfCore.apply(lambda x: (x.iloc[0] + x.iloc[1]) / 2, axis=1)

        # dfCore = pd.read_excel(path + "/inflation.xls", parse_dates=['date'])
        # dfCore.set_index('date', inplace=True)
        # return dfCore[['CPI:当月同比(月)', 'PPI:全部工业品:当月同比(月)']], dfCore[['GDP:平减指数:GDP:累计同比(季)']]

        sql_ct = "SELECT `TRADE_DT`, `CPI_Month`, `PPI_Industry_Month`, `GDP_Deflator` FROM `macroindex` ORDER BY `TRADE_DT` ASC;"
        dfCore = pd.read_sql(sql=sql_ct, con=self.conn_ct, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        # dfCore.sort_index(inplace=True)
        return dfCore[['CPI_Month', 'PPI_Industry_Month']], dfCore[['GDP_Deflator']]


class ERP1(base):

    def figure1data(self):
        sql_ct = 'SELECT `TRADE_DT`, `GDP:mainland` FROM `macroindex`;'
        # 上证综指: 000001.SH
        sql_wind = "SELECT `TRADE_DT`, `DIVIDEND_YIELD`, `PE_TTM` FROM aindexvaluation WHERE `S_INFO_WINDCODE` = '000001.SH';"
        sql_wind2 = "SELECT `TRADE_DT`, `S_DQ_CLOSE` FROM aindexeodprices WHERE `S_INFO_WINDCODE` = '000001.SH';"
        df_GDP = pd.read_sql(sql=sql_ct, con=self.conn_ct, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        df_nav = pd.read_sql(sql=sql_wind2, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        df_shangzheng = pd.read_sql(sql=sql_wind, con=self.conn_wind, parse_dates=['TRADE_DT'], index_col=['TRADE_DT'])
        df_nav.sort_index(inplace=True)
        df_shangzheng.sort_index(inplace=True)

        df_GDP_notNull = (df_GDP.copy()).dropna()
        df_GDP_notNull['Growth'] = pd.Series(None)
        df_GDP_notNull['Growth'] = (df_GDP_notNull['GDP:mainland'].pct_change()).fillna(0) * 100

        dateInterval = df_GDP_notNull.index
        df_nav_slice = df_nav.reindex(dateInterval | df_nav.index)
        df_nav_slice.fillna(method='ffill', inplace=True)
        df_nav_slice.fillna(method='bfill', inplace=True)
        df_nav_slice = df_nav_slice.reindex(dateInterval)
        df_shangzheng_slice = df_shangzheng.reindex(dateInterval)
        df_nav_slice['Growth'] = (df_nav_slice['S_DQ_CLOSE'].pct_change()).fillna(0) * 100

        figure1_data = pd.DataFrame(columns=['GDP_growth', 'Dividend_shangzheng', 'Shangzheng_growth'], index=dateInterval)
        figure1_data['GDP_growth'] = df_GDP_notNull['Growth']
        figure1_data['Dividend_shangzheng'] = df_shangzheng_slice['DIVIDEND_YIELD']
        figure1_data['Shangzheng_growth'] = df_nav_slice['Growth']

        return figure1_data


    def visualization(self):
        figure1_data = self.figure1data()
        params_basic = {
            'grid': True,
            'xlabel': 'Time/Freq=Month',
            'ylabel': 'Percentage',
            'title': 'GDP与上证指数比较',
            'xlim': None,
            'ylim': None
        }
        self.plotHandle.basic_line_single(data=figure1_data, **params_basic)


# 2.1 基于DDM模型计算的风险溢价
class ERP2_1(base):
    # def __init__(self):
    #     self.conn = DBconnection('wind')

    # @staticmethod
    # Simple Moving Average
    # def SMA(time_series, time_step):
    #     if time_step <= 1:
    #         return
    #     time_step = int(np.floor(time_step))
    #     res_ts = time_series.copy()
    #     for i in range(len(time_series))[time_step - 1:]:
    #         res_ts.iloc[i] = time_series.iloc[i - time_step + 1:i].sum() / time_step
    #
    #     # res_ts.iloc[:time_step] = np.nan
    #     return res_ts

    @staticmethod
    def SMA(time_series, time_step):
        return time_series.rolling(window=time_step).mean()

    # 2.1.1 股息率减无风险利率 (FED MODEL)

    # Ok
    def align_nominal_2111(self):
        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, bond10_df = self.bond()

        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        dateInterval = (hushen300_df.index & bond10_df.index) & (bond3_5_df.index & hushen300quan_df.index)

        hushen300_df = hushen300_df.reindex(dateInterval)
        bond3_5_df = bond3_5_df.reindex(dateInterval)
        bond10_df = bond10_df.reindex(dateInterval)
        hushen300quan_df = hushen300quan_df.reindex(dateInterval)

        figure1_data = pd.DataFrame(None,
                                    columns=['hushen300DividendRate-bond3_5YTM', 'hushen300DividendRate-bond10YTM',
                                             'hushen300Future1YReturn'], index=dateInterval)
        figure1_data['hushen300DividendRate-bond3_5YTM'] = hushen300_df['DIVIDEND_YIELD'] - bond3_5_df['AVG_CASH_YTM']
        figure1_data['hushen300DividendRate-bond10YTM'] = hushen300_df['DIVIDEND_YIELD'] - bond10_df['AVG_CASH_YTM']
        figure1_data['hushen300Future1YReturn'] = hushen300quan_df['FUTURE_1_Y_RETURN'] * 100

        figure2_data = pd.DataFrame(None, columns=['DividendTotal', 'DividendPaymentRate'])
        figure2_data['DividendTotal'] = hushen300_df['DIVIDEND_TOTAL']
        figure2_data['DividendPaymentRate'] = hushen300_df['DIVIDEND_YIELD_PAYMENT_RATE']

        return figure1_data, figure2_data

    def visualization_2111(self, figNum=1):
        figure1_data, figure2_data = self.align_nominal_2111()
        if figNum == 1:
            params_secondary = {
                'grid': True,
                'xlabel': 'Time/Freq=Day',
                'y1label': 'Percentage',
                'y2label': 'Percentage',
                'title': '股息率-名义无风险利率/沪深300未来一年收益',
                'xlim': None,
                'ylim': None
            }
            self.plotHandle.secondary_line_single(data1=figure1_data.iloc[:,0:2], data2=figure1_data.iloc[:,[-1]], **params_secondary)
        elif figNum == 2:
            param_bar = {
                'grid': False,
                'xlabel': 'Time/Freq:Day',
                'ylabel': '亿元',
                'title': '沪深300总分红/股息率',
                'xlim': None,
                'ylim': None
            }

            fig, ax1 = plt.subplots(figsize=self.plotHandle.plt_figsize['Middle'])
            plt.sca(ax=ax1)
            plt.bar(figure2_data.index, (figure2_data.iloc[:,0] / 100000000).values, width=0.5, color='r', label=figure2_data.columns[0])
            plt.grid(param_bar['grid'])
            plt.axis('tight')
            plt.xlabel(param_bar['xlabel'])
            plt.ylabel(param_bar['ylabel'])
            plt.title(param_bar['title'])
            plt.legend(loc=self.plotHandle.legend_loc['UpperRight'])

            ax2 = ax1.twinx()
            plt.sca(ax=ax2)
            plt.plot(figure2_data.index, (figure2_data.iloc[:,1] * 100).values, color='b', label=figure2_data.columns[1])
            plt.ylabel('Percentage')
            plt.legend(loc=self.plotHandle.legend_loc['LowerCenter'])

            plt.show()


    # Ok
    # def figure1Plot_2111(self):
    #     figure1_data, figure2_data = self.align_nominal_2111()
    #     # fig = plt.figure(figsize=(8,6))
    #     # axMy = fig.add_subplot(111)
    #     # axMy2 = axMy.twinx()
    #     #
    #     # figure1_data.plot(ax=axMy)
    #     # axMy.set_title('FED Model')
    #     # axMy.legend(['沪深300股息率-10Y国债', '沪深300股息率-1Y国债', '未来1年股票投资回报(rhs)'], loc='best')
    #     # axMy.set_xlabel('时间')
    #     #
    #     # plt.rcParams['font.sans-serif']=['SimHei']
    #     # # plt.savefig('risk3.png', dpi=400, bbox_inches='tight')
    #     # plt.show()
    #
    #     # figure1_data.astype(dtype=float)
    #     # figure1_data[['hushen300DividendRate-bond3_5YTM', 'hushen300DividendRate-bond10YTM']].plot()
    #     # figure1_data['hushen300Future1YReturn'].plot(secondary_y=True)
    #     # ax = figure1_data.plot(secondary_y=['hushen300Future1YReturn'])
    #     # plt.show()
    #
    #     fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)
    #     ax1.plot(figure1_data.index, figure1_data.iloc[:, 0].values, 'r-', figure1_data.index,
    #              figure1_data.iloc[:, 1].values, 'b.')
    #     # ax1.legend()
    #     ax1.set_title('股息率减无风险利率')
    #     ax1.set_ylabel('%')
    #
    #     ax3.plot(figure1_data.index, figure1_data.iloc[:, 2].values, 'g')
    #     ax3.set_title('未来一年收益(rhs)')
    #     ax3.set_ylabel('%')
    #
    #     ax2.plot(figure2_data.index, figure2_data['DividendTotal'] / 100000000)
    #     ax2.set_title('现金分红总额')
    #     ax2.set_ylabel('亿元')
    #
    #     ax4.plot(figure2_data.index, figure2_data['DividendPaymentRate'])
    #     ax4.set_title('股息支付率')
    #     ax4.set_ylabel('%')
    #
    #     plt.show()

    # Ok
    def align_real_2112(self):
        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, bond10_df = self.bond()
        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        dateInterval = (hushen300_df.index & bond10_df.index) & (bond3_5_df.index & hushen300quan_df.index)

        hushen300_df = hushen300_df.reindex(dateInterval)
        bond3_5_df = bond3_5_df.reindex(dateInterval)
        bond10_df = bond10_df.reindex(dateInterval)
        hushen300quan_df = hushen300quan_df.reindex(dateInterval)

        hushen300quan_df['FUTURE_1_Y_RETURN'] = pd.Series()
        for index in hushen300quan_df.index:
            future = index + timedelta(days=250)
            if future in hushen300quan_df.index:
                hushen300quan_df.loc[index, 'FUTURE_1_Y_RETURN'] = hushen300quan_df.loc[future, 'S_DQ_CLOSE'] / \
                                                                   hushen300quan_df.loc[index, 'S_DQ_CLOSE'] - 1
        hushen300quan_df['FUTURE_1_Y_RETURN'].fillna(method='ffill', inplace=True)

        # 2016 - 2018
        # inflation_df = self.inflation()
        # inflation_df = inflation_df.reindex(dateInterval)
        # inflation_df.fillna(method='ffill', inplace=True)
        # inflation_df.fillna(method='bfill', inplace=True)

        CPI_PPI_df, GDPpingjian_df = self.inflation()
        CPI_PPI_df = CPI_PPI_df.reindex(dateInterval)
        CPI_PPI_df.fillna(method='ffill', inplace=True)
        CPI_PPI_df.fillna(method='bfill', inplace=True)

        GDPpingjian_df = GDPpingjian_df.reindex(dateInterval)
        GDPpingjian_df.fillna(method='ffill', inplace=True)
        GDPpingjian_df.fillna(method='bfill', inplace=True)

        inflation_df = CPI_PPI_df.copy()
        # inflation_df['Inflation'] = (inflation_df['CPI:当月同比(月)'] + inflation_df['PPI:全部工业品:当月同比(月)']) / 2
        # 第一列是 CPI, 第二列是 PPI
        inflation_df['Inflation'] = (inflation_df.iloc[:,0] + inflation_df.iloc[:,1]) / 2

        figure1_data = pd.DataFrame(None,
                                    columns=['hushen300DividendRate-bond3_5YTM', 'hushen300DividendRate-bond10YTM',
                                             'hushen300Future1YReturn'], index=dateInterval)
        figure1_data['hushen300DividendRate-bond3_5YTM'] = hushen300_df['DIVIDEND_YIELD'] - bond3_5_df['AVG_CASH_YTM'] + \
                                                           inflation_df['Inflation']
        figure1_data['hushen300DividendRate-bond10YTM'] = hushen300_df['DIVIDEND_YIELD'] - bond10_df['AVG_CASH_YTM'] + \
                                                          inflation_df['Inflation']
        figure1_data['hushen300Future1YReturn'] = hushen300quan_df['FUTURE_1_Y_RETURN'] * 100

        figure2_data = pd.DataFrame({
            'GDP:平减指数:GDP:累计同比(季)': GDPpingjian_df.iloc[:,0],
            'Inflation': inflation_df['Inflation']
        }, index=inflation_df.index)

        return figure1_data, figure2_data

    def visualization_2112(self, figNum=1):
        figure1_data, figure2_data = self.align_real_2112()

        if figNum == 1:
            params_secondary = {
                'grid': True,
                'xlabel': 'Time/Freq=Day',
                'y1label': 'Percentage',
                'y2label': 'Percentage',
                'title': '股息率-实际无风险利率',
                'xlim': None,
                'ylim': None
            }

            self.plotHandle.secondary_line_single(data1=figure1_data.iloc[:,0:2], data2=figure1_data.iloc[:,[-1]], **params_secondary)

        elif figNum == 2:
            params_basic = {
                'grid': True,
                'xlabel': 'Time/Freq=Day',
                'ylabel': '%',
                'title': 'CPI和PPI拟合通胀指数',
                'xlim': None,
                'ylim': None
            }

            figure2_data['label'] = pd.Series(None)
            for index in figure2_data.index:
                figure2_data.loc[index, 'label'] = str(index)[:7]

            df_month = figure2_data.groupby('label').mean()
            df_month.set_index(pd.to_datetime(df_month.index), inplace=True)
            self.plotHandle.basic_line_single(data=df_month, **params_basic)

    # Ok
    # def figure12Plot_2112(self):
    #     figure1_data, figure2_data = self.align_real_2111()
    #
    #     figure1_data.astype(dtype=float)
    #     figure1_data[['hushen300DividendRate-bond3_5YTM', 'hushen300DividendRate-bond10YTM']].plot()
    #     figure1_data['hushen300Future1YReturn'].plot(secondary_y=True)
    #     ax1 = figure1_data.plot(secondary_y=['hushen300Future1YReturn'])
    #
    #     ax2 = figure2_data.plot()
    #     plt.show()
    #     # fig, (ax1, ax2) = plt.subplot(1,2)
    #     # figure1_data.plot(ax=ax1, figsize=(8,6), title='股息率减无风险利率', secondary_y=figure1_data['hushen300Future1YReturn'])
    #     # ax2.bar(x=figure2_data.index, y=figure2_data['DividendTotal'])
    #     # ax2.plot(x=figure2_data.index, y=figure2_data['DividendPaymentRate'])
    #     #
    #     # plt.legend()
    #     # plt.show()
    #
    #     # fig = plt.figure(figsize=(8, 6))
    #     # axMy = fig.add_subplot(111)
    #     #
    #     # figure1_data.plot(ax=axMy)
    #     # axMy.set_title('FED Model')
    #     # axMy.legend(['沪深300股息率-10Y国债真实利率', '沪深300股息率-1Y国债真实利率', '未来1年股票投资回报(rhs)'], loc='best')
    #     # axMy.set_xlabel('时间')
    #     #
    #     # plt.rcParams['font.sans-serif'] = ['SimHei']
    #     # # plt.savefig('risk3.png', dpi=400, bbox_inches='tight')
    #     # plt.show()

    # 2.1.2 用市盈率的倒数减国债收益率 名义/实际
    # PE_TTM: 静态市盈率
    # Ok
    def align_TTM_2121(self, real=False):
        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, _ = self.bond()
        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        dateInterval = (hushen300_df.index & hushen300quan_df.index) & bond3_5_df.index

        hushen300_df = hushen300_df.reindex(dateInterval)
        bond3_5_df = bond3_5_df.reindex(dateInterval)
        hushen300quan_df = hushen300quan_df.reindex(dateInterval)

        figure1_data = pd.DataFrame(None, columns=['ERP', 'ERP_SMA', '+2SD', '-2SD', 'Future1YReturn'],
                                    index=dateInterval)
        if real:
            CPI_PPI_df, GDPpingjian_df = self.inflation()
            CPI_PPI_df = CPI_PPI_df.reindex(dateInterval)
            CPI_PPI_df.fillna(method='ffill', inplace=True)
            CPI_PPI_df.fillna(method='bfill', inplace=True)

            inflation_df = CPI_PPI_df.copy()
            # inflation_df['Inflation'] = (inflation_df['CPI:当月同比(月)'] + inflation_df['PPI:全部工业品:当月同比(月)']) / 2
            inflation_df['Inflation'] = (inflation_df.iloc[:, 0] + inflation_df.iloc[:, 1]) / 2

            figure1_data['ERP'] = (1 / hushen300_df['PE_TTM']) - (
                        (bond3_5_df['AVG_CASH_YTM'] - inflation_df['Inflation']) / 100)
        else:
            figure1_data['ERP'] = (1 / hushen300_df['PE_TTM']) - (bond3_5_df['AVG_CASH_YTM'] / 100)

        figure1_data['ERP_SMA'] = self.SMA(time_series=figure1_data['ERP'], time_step=250)
        # figure1_data['ERP_SMA'] = figure1_data['ERP'].rolling(window=250).mean()
        # std = figure1_data['ERP_SMA'].std()
        figure1_data['+2SD'] = figure1_data['ERP_SMA'] + 2 * figure1_data['ERP_SMA'].rolling(window=100).std()
        figure1_data['-2SD'] = figure1_data['ERP_SMA'] - 2 * figure1_data['ERP_SMA'].rolling(window=100).std()
        figure1_data['Future1YReturn'] = hushen300quan_df['FUTURE_1_Y_RETURN']

        return figure1_data

    def visualization_2121(self, real=False):
        params_secondary = {
            'grid': True,
            'xlabel': 'Time/Freq=Day',
            'y1label': 'Percentage',
            'y2label': 'Percentage',
            'title': '静态市盈率-无风险利率' + ('(名义)' if real == False else '(实际)'),
            'xlim': None,
            'ylim': None
        }

        figure1_data = self.align_TTM_2121(real=real)
        self.plotHandle.secondary_line_single(data1=figure1_data.iloc[:,0:4], data2=figure1_data.iloc[:, [-1]], **params_secondary)



    #  待修复 real部分
    # Ok
    # def figure1Plot_2121(self):
    #     figure1_data = self.align_TTM_2121(real=False)
    #
    #     # 条件截取
    #     figure1_data = figure1_data[figure1_data.index > '20100101']
    #
    #     figure1_data.astype(dtype=float)
    #
    #     figure1_data[['ERP', 'ERP_SMA', '+2SD', '-2SD']].plot()
    #     figure1_data[['Future1YReturn']].plot(secondary_y=True)
    #     ax1 = figure1_data.plot(secondary_y=['Future1YReturn'])
    #
    #     plt.show()

    # EST_PE_Y1: 动态市盈率
    # Ok
    def align_EST_PE_2122(self, real=False):
        '''
        figure1_data: ['ERP_FutureEP_nominal', 'ERP_FutureEP_real', 'Future1YReturn']
        :return:
        '''
        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, _ = self.bond()
        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        dateInterval = (hushen300_df.index & hushen300quan_df.index) & bond3_5_df.index

        hushen300_df = hushen300_df.reindex(dateInterval)
        bond3_5_df = bond3_5_df.reindex(dateInterval)
        hushen300quan_df = hushen300quan_df.reindex(dateInterval)

        # inflation_df = inflation_df.reindex(dateInterval)
        # inflation_df.fillna(method='ffill', inplace=True)
        # inflation_df.fillna(method='bfill', inplace=True)

        CPI_PPI_df, GDPpingjian_df = self.inflation()
        CPI_PPI_df = CPI_PPI_df.reindex(dateInterval)
        CPI_PPI_df.fillna(method='ffill', inplace=True)
        CPI_PPI_df.fillna(method='bfill', inplace=True)

        inflation_df = CPI_PPI_df.copy()
        # inflation_df['Inflation'] = (inflation_df['CPI:当月同比(月)'] + inflation_df['PPI:全部工业品:当月同比(月)']) / 2
        inflation_df['Inflation'] = (inflation_df.iloc[:,0] + inflation_df.iloc[:,1]) / 2

        figure1_data = pd.DataFrame(None, columns=['ERP_FutureEP_nominal', 'ERP_FutureEP_real', 'Future1YReturn'],
                                    index=dateInterval)
        figure1_data['ERP_FutureEP_nominal'] = (1 / hushen300_df['EST_PE_Y1']) - (bond3_5_df['AVG_CASH_YTM'] / 100)
        figure1_data['ERP_FutureEP_real'] = (1 / hushen300_df['EST_PE_Y1']) - (
                    (bond3_5_df['AVG_CASH_YTM'] - inflation_df['Inflation']) / 100)
        figure1_data['Future1YReturn'] = hushen300quan_df['FUTURE_1_Y_RETURN']

        return figure1_data

    def visualization_2122(self, real=False):
        params_secondary = {
            'grid': True,
            'xlabel': 'Time/Freq=Day',
            'y1label': 'Percentage',
            'y2label': 'Percentage',
            'title': '动态市盈率-无风险利率' + ('(名义)' if real == False else '(实际)'),
            'xlim': None,
            'ylim': None
        }

        figure1_data = self.align_EST_PE_2122(real=real)
        self.plotHandle.secondary_line_single(data1=figure1_data.iloc[:, 0:4], data2=figure1_data.iloc[:, [-1]],
                                              **params_secondary)






    # Ok
    # def figure12Plot_2122(self):
    #     figure1_data = self.align_EST_PE_2122()
    #
    #     # 条件截取
    #     figure1_data = figure1_data[figure1_data.index > '20130101']
    #
    #     figure1_data.astype(dtype=float)
    #
    #     # ['ERP_FutureEP_nominal', 'ERP_FutureEP_real', 'Future1YReturn']
    #     figure1_data[['ERP_FutureEP_nominal']].plot()
    #     figure1_data[['Future1YReturn']].plot(secondary_y=True)
    #     ax1 = figure1_data.plot(secondary_y=['Future1YReturn'])
    #
    #     plt.show()

    # CAPE: cyclically adjusted PE
    # Ok
    def align_CAPE_2123(self):
        '''
        hushen300_df.columns=
        ['DIVIDEND_YIELD', 'PE_TTM', 'MV_FLOAT', 'EST_PE_Y1', 'PE_LYR',
       'DIVIDEND_TOTAL', 'DIVIDEND_YIELD_PAYMENT_RATE']
        :return:
        '''
        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, bond10_df = self.bond()
        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        dateInterval = (hushen300_df.index & bond10_df.index) & (bond3_5_df.index & hushen300quan_df.index)

        hushen300_df = hushen300_df.reindex(dateInterval)
        bond3_5_df = bond3_5_df.reindex(dateInterval)
        bond10_df = bond10_df.reindex(dateInterval)
        hushen300quan_df = hushen300quan_df.reindex(dateInterval)

        # inflation_df = self.inflation()
        # inflation_df = inflation_df.reindex(dateInterval)
        # inflation_df.fillna(method='ffill', inplace=True)
        # inflation_df.fillna(method='bfill', inplace=True)

        CPI_PPI_df, GDPpingjian_df = self.inflation()
        CPI_PPI_df = CPI_PPI_df.reindex(dateInterval)
        CPI_PPI_df.fillna(method='ffill', inplace=True)
        CPI_PPI_df.fillna(method='bfill', inplace=True)

        inflation_df = CPI_PPI_df.copy()
        # inflation_df['Inflation'] = (inflation_df['CPI:当月同比(月)'] + inflation_df['PPI:全部工业品:当月同比(月)']) / 2
        inflation_df['Inflation'] = (inflation_df.iloc[:, 0] + inflation_df.iloc[:,1]) / 2
        # PE_TTM
        # hushen300_df['CAPE'] = hushen300_df['PE_TTM'] / (1 + inflation_df['Inflation'] / 100)

        CAPE_df = pd.DataFrame(None, columns=['S_DQ_CLOSE', 'earnings', 'Inflation'], index=dateInterval)
        CAPE_df['S_DQ_CLOSE'] = hushen300quan_df['S_DQ_CLOSE']
        CAPE_df['earnings'] = hushen300_df['MV_FLOAT'] / hushen300_df['PE_TTM']
        CAPE_df['Inflation'] = inflation_df['Inflation']

        rollingYear = 1
        CAPE_df['CAPE'] = (CAPE_df['S_DQ_CLOSE'] / CAPE_df['Inflation'] * hushen300_df['TOT_SHR_FLOAT']) / (
            (CAPE_df['earnings'] / CAPE_df['Inflation']).rolling(window=rollingYear * 250).mean())

        figure1_data = pd.DataFrame(None, columns=['CAPE10Y', 'CAPE3_5Y', 'Future1YReturn'], index=dateInterval)
        figure1_data['CAPE10Y'] = (1 / CAPE_df['CAPE']) - (bond10_df['AVG_CASH_YTM'] / 100)
        figure1_data['CAPE3_5Y'] = (1 / CAPE_df['CAPE']) - (bond3_5_df['AVG_CASH_YTM'] / 100)
        figure1_data['Future1YReturn'] = hushen300quan_df['FUTURE_1_Y_RETURN']

        return figure1_data

    # Ok
    def figure1Plot_2123(self):
        figure1_data = self.align_CAPE_2123()

        # 条件截取
        figure1_data = figure1_data[figure1_data.index > '20050101']

        figure1_data.astype(dtype=float)

        # ['ERP_FutureEP_nominal', 'ERP_FutureEP_real', 'Future1YReturn']
        figure1_data[['CAPE10Y', 'CAPE3_5Y']].plot()
        figure1_data[['Future1YReturn']].plot(secondary_y=True)
        ax1 = figure1_data.plot(secondary_y=['Future1YReturn'])

        plt.show()

    # 2.1.3 两阶段及四阶段DDM公式
    # data not good ...
    def DDM2_2131(self):
        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, _ = self.bond()
        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        growth_df = pd.DataFrame(None, columns=['g_s_earnings', 'g_s_PE', 'g_l'], index=hushen300_df.index)
        growth_df['PE_TTM'] = hushen300_df['PE_TTM']
        growth_df['MV_FLOAT'] = hushen300_df['MV_FLOAT']
        growth_df['earnings'] = growth_df['MV_FLOAT'] / growth_df['PE_TTM']

        start = growth_df.index[0]
        for index in growth_df.index:
            delta = timedelta(days=250 * 5)
            if (index - delta) in growth_df.index:
                growth_df.loc[index, 'g_s_earnings'] = (growth_df.loc[index, 'earnings'] - growth_df.loc[
                    index - delta, 'earnings']) / growth_df.loc[index - delta, 'earnings']
            growth_df.loc[index, 'g_l'] = (growth_df.loc[index, 'earnings'] - growth_df.loc[start, 'earnings']) / \
                                          growth_df.loc[start, 'earnings']

        # g_s = 0.68 - 0.01*g_s_earnings^(-1) - 0.048*PE_TTM
        growth_df['g_s'] = 0.68 - 0.01 * (1 / growth_df['g_s_earnings']) - 0.048 * growth_df['PE_TTM']

        # g_s, g_l 单位为 1

        dateInterval = (growth_df.index & bond3_5_df.index) & hushen300quan_df.index

        bond3_5_df = bond3_5_df.reindex(dateInterval)
        hushen300quan_df = hushen300quan_df.reindex(dateInterval)
        growth_df = growth_df.reindex(dateInterval)

        H = 5
        D0 = hushen300_df.loc[dateInterval[0], 'DIVIDEND_TOTAL']
        idealPrice = pd.DataFrame({'g_s': growth_df['g_s'], 'g_l': growth_df['g_l']}, index=dateInterval)
        idealPrice.fillna(method='ffill', inplace=True)
        idealPrice.fillna(method='bfill', inplace=True)
        numerator = (1 + idealPrice['g_l']) + H * (idealPrice['g_s'] - idealPrice['g_l'])
        idealPrice['idealNav'] = (D0 * numerator) / (bond3_5_df['AVG_CASH_YTM'] / 100 - idealPrice['g_l'])

        # 有负值，待解决
        idealPrice['ERP'] = idealPrice['idealNav'].pct_change() - (bond3_5_df['AVG_CASH_YTM'] / 100)

        figure1data = pd.DataFrame(
            {'ERP': idealPrice['ERP'], 'FUTURE_1Y_RETURN': hushen300quan_df['FUTURE_1_Y_RETURN']},
            index=dateInterval)
        return figure1data

    def figure1Plot_2131(self):
        figure1_data = self.DDM2_2131()

        # 条件截取
        figure1_data = figure1_data[figure1_data.index > '20090101']

        figure1_data.astype(dtype=float)

        # ['ERP_FutureEP_nominal', 'ERP_FutureEP_real', 'Future1YReturn']
        figure1_data[['ERP']].plot()
        figure1_data[['FUTURE_1Y_RETURN']].plot(secondary_y=True)
        ax1 = figure1_data.plot(secondary_y=['FUTURE_1Y_RETURN'])

        plt.show()

    # Ok
    def DDM4_2132(self):
        # EST_YOYPROFIT_Y1 预测净利润同比增速(Y1)
        sqlMy = "SELECT TRADE_DT, EST_YOYPROFIT_Y1, EST_YOYPROFIT_Y2 FROM aindexvaluation WHERE S_INFO_WINDCODE = '000300.SH';"
        YOYPROFIT_df = pd.read_sql(sql=sqlMy, con=self.conn_wind, parse_dates=['TRADE_DT'])
        YOYPROFIT_df.set_index('TRADE_DT', inplace=True)
        YOYPROFIT_df.sort_index(inplace=True)
        YOYPROFIT_df = YOYPROFIT_df.apply(lambda x: x / 100, axis=0)
        # 预测净利润同比增速(Y3) = [ 预测净利润同比增速(Y1) + 预测净利润同比增速(Y2) ] / 2
        YOYPROFIT_df['EST_YOYPROFIT_Y3'] = (YOYPROFIT_df['EST_YOYPROFIT_Y1'] + YOYPROFIT_df['EST_YOYPROFIT_Y2']) / 2
        # 单位为 1

        YOYPROFIT_df = YOYPROFIT_df.rename(
            columns={'EST_YOYPROFIT_Y1': 'g1', 'EST_YOYPROFIT_Y2': 'g2', 'EST_YOYPROFIT_Y3': 'g3'})
        YOYPROFIT_df['g_l'] = pd.Series(None)
        # 长期增速设为常数 0.01，如果大于无风险利率，term4 级数不收敛
        YOYPROFIT_df.fillna(0.01, inplace=True)

        hushen300_df, hushen300quan_df = self.hushen300()
        # bond3_5_df, _ = self.bond()

        bond_description = ('中债-3-5年期国债指数', '中债-10年期国债指数')
        bond_dict = self.bond(description=bond_description)
        bond3_5_df = bond_dict[bond_description[0]]
        bond10_df = bond_dict[bond_description[1]]

        dateInterval = (hushen300_df.index & hushen300quan_df.index) & (YOYPROFIT_df.index & bond3_5_df.index)

        hushen300quan_df = hushen300quan_df.reindex(dateInterval)
        bond3_5_df = bond3_5_df.reindex(dateInterval)
        YOYPROFIT_df = YOYPROFIT_df.reindex(dateInterval)

        DE0 = hushen300_df.loc[dateInterval[0], 'DIVIDEND_YIELD_PAYMENT_RATE']
        bond3_5_df['AVG_CASH_YTM'] = bond3_5_df['AVG_CASH_YTM'] / 100

        figure1data = pd.DataFrame(columns=['ERP', 'FUTURE_1Y_RETURN'], index=dateInterval)
        figure1data['FUTURE_1Y_RETURN'] = hushen300quan_df['FUTURE_1_Y_RETURN']
        term1 = DE0 * (1 + YOYPROFIT_df['g1']) / (1 + bond3_5_df['AVG_CASH_YTM'])
        term2 = term1 * (1 + YOYPROFIT_df['g2']) / (1 + bond3_5_df['AVG_CASH_YTM'])
        term3 = term2 * (1 + YOYPROFIT_df['g3']) / (1 + bond3_5_df['AVG_CASH_YTM'])
        # 等比数列求和
        commonRatio = (1 + YOYPROFIT_df['g_l']) / (1 + bond3_5_df['AVG_CASH_YTM'])
        term4 = term3 * commonRatio / (1 - commonRatio)
        figure1data['ERP'] = 1 / (term1 + term2 + term3 + term4)

        return figure1data

    # Ok
    def figure1Plot_2132(self):
        figure1_data = self.DDM4_2132()

        # 条件截取
        figure1_data = figure1_data[figure1_data.index > '20150101']

        figure1_data.astype(dtype=float)

        # ['ERP_FutureEP_nominal', 'ERP_FutureEP_real', 'Future1YReturn']
        figure1_data[['ERP']].plot()
        figure1_data[['FUTURE_1Y_RETURN']].plot(secondary_y=True)
        ax1 = figure1_data.plot(secondary_y=['FUTURE_1Y_RETURN'])

        plt.show()

# -------------------------------------













# plotHandle = plot_CT()
# ins = ERP1()
# ins.visualization()


# figure1_data = ins.figure1data()
# params_basic = {
#     'grid': True,
#     'xlabel': 'Time/Freq=Month',
#     'ylabel': 'Percentage',
#     'title': 'GDP与上证指数比较',
#     'xlim': None,
#     'ylim': None
# }
# plotHandle.basic_line_single(data=figure1_data, **params_basic)





# np.random.seed(1000)
# # y = np.random.standard_normal((20,2))
# # x = range(len(y))
# # dateInterval = pd.date_range(start='20100101', periods=20, freq='D')
# # dfCore = pd.DataFrame(y, columns=['Col_1', 'Col_2'], index=dateInterval)
# # params_basic = {
# #     'grid': True,
# #     'xlabel': '时间',
# #     'ylabel': 'Value',
# #     'title': '实验1',
# #     'xlim': None,
# #     'ylim': (-2, 2)
# # }
# # ins.basic_line_single(data=dfCore, **params_basic)
# #
# #
# # ins_ERP = ERP2_1()
# # figure1_data, figure2_data = ins_ERP.align_nominal_2111()
# #
# # params_secondary = {
# #     'grid': True,
# #     'xlabel': '时间',
# #     'y1label': '%',
# #     'y2label': '%',
# #     'title': 'ERP No.1',
# #     'xlim': None,
# #     'ylim': None
# # }
# # ins.secondary_line_single(data1=figure1_data[['hushen300DividendRate-bond3_5YTM', 'hushen300DividendRate-bond10YTM']],
# #                           data2=figure1_data[['hushen300Future1YReturn']], **params_secondary)
# #
# #
# #
# # params_basic_bar = {
# #     'grid': True,
# #     'xlabel': '时间',
# #     'ylabel': '亿元',
# #     'title': '现金股利分红',
# #     'xlim': None,
# #     'ylim': None
# # }
# # df = (figure2_data[['DividendTotal']]/100000000).iloc[:20, :]
# # ins.basic_bar_single(data=df.apply(lambda x: x.cumsum()))




# figure2_data['label'] = pd.Series(None)
# for index in figure2_data.index:
#     figure2_data.loc[index, 'label'] = str(index)[:7]
#
# df = figure2_data.groupby('label').mean()

# ------------------------- begin here !!!!!!!!!!!!!!!!
# ins = ERP2_1()
# ins.visualization_2121(real=True)
ins_plot = plot_CT()

# df = pd.read_csv('cashportfolio.csv', parse_dates=['td_date'], index_col=['td_date'])
# ins_plot.basic_line_single(data=df * 100)
# df2 = pd.read_csv('cashportfolio_another.csv',  parse_dates=['td_date'], index_col=['td_date'])
# ins_plot.basic_line_single(data=df2 * 100)
#
# df3 = pd.read_csv('THREE.csv',  parse_dates=['td_date'], index_col=['td_date'])
# ins_plot.basic_line_single(data=df3 * 100)
#
# def A(path):
#     df = pd.read_csv(path, parse_dates=['td_date'], index_col=['td_date'])
#     ins_plot.basic_line_single(data=df * 100)
#     return
#
# df4 = pd.read_csv(r'E:\1-Career\lcmf\Projects\CurrencyFundAnalysis\BondTotal.csv', parse_dates=['td_date'], index_col=['td_date'])
# df5 = pd.read_csv(r'E:\1-Career\lcmf\Projects\CurrencyFundAnalysis\BondTHREE.csv', parse_dates=['td_date'], index_col=['td_date'])
# ins_plot.basic_line_two(data1=df4.iloc[:,[1,-1]] * 100 , data2=df4.iloc[:, [0,3]], title='债券总占比')
# ins_plot.basic_line_two(data1=df5.iloc[:,[1,-1]]  * 100, data2=df5.iloc[:, [0,3]], title='三类特殊债券')
#
#
# df6 = pd.read_csv(r'E:\1-Career\lcmf\Projects\CurrencyFundAnalysis\BondTotal_2.csv', parse_dates=['td_date'], index_col=['td_date'])
# df7 = pd.read_csv(r'E:\1-Career\lcmf\Projects\CurrencyFundAnalysis\BondThree_2.csv', parse_dates=['td_date'], index_col=['td_date'])
# ins_plot.basic_line_two(data1=df6.iloc[:,[1,-1]]  , data2=df6.iloc[:, [0,3]], title='债券总占比')
# ins_plot.basic_line_two(data1=df7.iloc[:,[1,-1]] , data2=df7.iloc[:, [0,3]], title='三类特殊债券')


df8 = pd.read_csv(r'E:\1-Career\lcmf\Projects\CurrencyFundAnalysis\NewFourTotal_4.csv', parse_dates=['td_date'], index_col=['td_date'])
df9 = pd.read_csv(r'E:\1-Career\lcmf\Projects\CurrencyFundAnalysis\NewFourFour_4.csv', parse_dates=['td_date'], index_col=['td_date'])
df8['portfolio_4'] = df9.iloc[:,0]
ins_plot.basic_line_single(data=df8)
ins_plot.basic_line_single(data=df9, title='四类债券总和')


