# -*- coding:utf-8 -*-
# !/usr/local/bin/python
############################################################################
'''''
# 程序： Build Fund_Analysis Class
# 功能：Build Fund_Analysis Class to analyse funds
# 创建时间：2018/04/11
# 更新历史：
#
# 使用库：requests、BeautifulSoup4、pymysql,pandas
# 作者：Guan Hao
'''
#############################################################################

import C_MySQL_Server as db
from sqlalchemy import select
from C_Fund import FundInstance
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
import pickle

df_funds = pd.DataFrame()


def loadFundCodesFromPickle(path):
    with open(path, 'rb') as f:
        df_funds['fund_code'] = list(pickle.load(f))

    return df_funds


def loadFundsNavInDf(before):
    df_funds_nav = pd.DataFrame()
    df_funds_list = loadFundCodesFromPickle('selected_funds.ticker')

    i = 0
    count = df_funds_list.count(axis=0)
    before_date = datetime.strptime(before, '%Y-%m-%d')

    for fund in df_funds_list['fund_code']:
        if i % 10 == 0:
            print ('{}/{}'.format(i, count))
        i += 1

        fi = FundInstance(fund)
        df_single_nav = fi.getUnitNetValueList(fund).loc[:, ['quote_date', 'unit_net_value']]
        df_single_nav.rename(columns={'unit_net_value': '{}_Nav'.format(fund)}, inplace=True)
        df_single_nav = df_single_nav.loc[df_single_nav['quote_date'] > before_date, :]

        if df_funds_nav.empty:
            df_funds_nav = df_single_nav
        else:
            df_funds_nav = pd.DataFrame.merge(df_funds_nav, df_single_nav, how='outer', on=['quote_date', 'quote_date'])

    df_funds_nav.to_csv('fund_nav.csv', header=True, sep=',')


def readFundsNavFromCSV():
    df_funds_nav = pd.DataFrame.from_csv('fund_nav.csv')
    df_funds_nav.fillna(0, inplace=True)

    # x_d = df_funds_nav['quote_date']
    # y_d = df_funds_nav['000003_Nav']
    # plt.plot(x_d, y_d, color='r', linewidth=2.0)
    df_funds_nav.plot(x='quote_date', y=[y for y in df_funds_nav.iloc[:, 1:10]], figsize=(100, 500))
    plt.show()


def main():
    # loadFundCodesFromPickle('selected_funds.ticker')
    # loadFundsNavInDf('2015-01-01')
    readFundsNavFromCSV()


if __name__ == "__main__":
    main()
