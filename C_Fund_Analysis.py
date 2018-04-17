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
import numpy as np
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
from matplotlib import cm
import pickle

df_funds = pd.DataFrame()


class C_Fund_Analysis():
    def loadFundCodesFromPickle(self, path):
        with open(path, 'rb') as f:
            df_funds['fund_code'] = list(pickle.load(f))

        return df_funds

    def loadFundsNavInDf(self, before):
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
                df_funds_nav = pd.DataFrame.merge(df_funds_nav, df_single_nav, how='outer',
                                                  on=['quote_date', 'quote_date'])

        df_funds_nav.to_csv('fund_nav.csv', header=True, sep=',')

    def readFundsNavFromCSV(self):
        df_funds_nav = pd.read_csv('fund_nav.csv', index_col=1).iloc[:, 1:]
        df_funds_nav.fillna(0.0, inplace=True)
        df_funds_nav.replace(to_replace=r'\s+', value=0.0, regex=True, inplace=True)
        df_funds_nav.index = df_funds_nav.index.astype(np.datetime64)
        df_funds_nav = df_funds_nav.iloc[:, :].astype(np.float64)
        return df_funds_nav

    def fundsCorr(self, df_funds_nav):
        df_corr = df_funds_nav.corr()
        return df_corr

    def plotCorrHeadMap(self, df_corr):
        df_corr_values = df_corr.values
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)

        heatmap = ax.pcolor(df_corr_values, cmap=plt.cm.RdYlGn)
        fig.colorbar(heatmap)
        ax.set_xticks(np.arange(df_corr_values.shape[0]) + 0.5, minor=False)
        ax.set_yticks(np.arange(df_corr_values.shape[1]) + 0.5, minor=False)
        ax.invert_yaxis()
        ax.xaxis.tick_top()

        column_labels = df_corr.columns
        row_labels = df_corr.index

        ax.set_xticklabels(column_labels)
        ax.set_yticklabels(row_labels)
        plt.xticks(rotation=90)
        heatmap.set_clim(-1, 1)
        plt.tight_layout()
        plt.show()

    def plotFunds(self, df_funds_nav):
        # x_d = df_funds_nav['quote_date']
        # y_d = df_funds_nav['000003_Nav']
        # plt.plot(x_d, y_d, color='r', linewidth=2.0)
        df_funds_nav.plot(x='quote_date', y=[y for y in df_funds_nav.iloc[:, 1:10]], figsize=(100, 500))
        plt.show()




def main():
    # loadFundCodesFromPickle('selected_funds.ticker')
    # loadFundsNavInDf('2015-01-01')
    fa = C_Fund_Analysis()
    df_nav = fa.readFundsNavFromCSV()
    # df_corr = fa.fundsCorr(df_nav.iloc[:, 0:30])
    df_corr = fa.fundsCorr(df_nav)
    fa.plotCorrHeadMap(df_corr)


if __name__ == "__main__":
    main()
