import pymysql
import pandas as pd
import numpy as np
from dateutil.parser import parse
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import statsmodels.api as sm


def DBconnection(DBname):
    '''
    Choose 'DBname' between: wind, mofang
    '''
    if DBname == 'wind':
        conn = pymysql.connect(host='192.168.88.11', user='public', passwd='h76zyeTfVqAehr5J', database='wind', charset='utf8')
    else:
        conn = pymysql.connect(host='192.168.88.254', user='root', passwd='Mofang123', database='mofang', charset='utf8')
    return conn


def writeExcel(data: pd.DataFrame, path: str, sheet_name: str):
    '''Save DataFrame to path as .xls

    :param data:
    :param path:
    :param sheet_name:
    :return:
    '''
    with pd.ExcelWriter(path) as ExcelWriter:
        data.to_excel(ExcelWriter, sheet_name=sheet_name)
        ExcelWriter.save()


