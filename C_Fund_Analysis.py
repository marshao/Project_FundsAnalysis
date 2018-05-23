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

    def loadFundsNavInCSV(self, before, path):
        df_funds_nav = pd.DataFrame()
        df_funds_list = self.loadFundCodesFromPickle(path)

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

    def loadFundsCumNavInCSV(self, before, path):
        df_funds_nav = pd.DataFrame()
        df_funds_list = self.loadFundCodesFromPickle(path)

        i = 0
        count = df_funds_list.count(axis=0)
        before_date = datetime.strptime(before, '%Y-%m-%d')

        for fund in df_funds_list['fund_code']:
            if i % 10 == 0:
                print ('{}/{}'.format(i, count))
            i += 1

            fi = FundInstance(fund)
            df_single_nav = fi.getUnitNetValueList(fund).loc[:, ['quote_date', 'cum_net_value']]
            df_single_nav.rename(columns={'cum_net_value': '{}_Nav'.format(fund)}, inplace=True)
            df_single_nav = df_single_nav.loc[df_single_nav['quote_date'] > before_date, :]

            if df_funds_nav.empty:
                df_funds_nav = df_single_nav
            else:
                df_funds_nav = pd.DataFrame.merge(df_funds_nav, df_single_nav, how='outer',
                                                  on=['quote_date', 'quote_date'])

        df_funds_nav.to_csv('fund_cum_nav.csv', header=True, sep=',', na_rep='NA', float_format='%.4f', index=0)

    def loadFundsChgInCSV(self, before, path):
        df_funds_nav = pd.DataFrame()
        df_funds_list = self.loadFundCodesFromPickle(path)

        i = 0
        count = df_funds_list.count(axis=0)
        before_date = datetime.strptime(before, '%Y-%m-%d')

        for fund in df_funds_list['fund_code']:
            if i % 10 == 0:
                print ('{}/{}'.format(i, count))
            i += 1

            fi = FundInstance(fund)
            df_single_nav = fi.getUnitNetValueList(fund).loc[:, ['quote_date', 'daily_chg_rate']]
            df_single_nav.rename(columns={'daily_chg_rate': '{}_Chg'.format(fund)}, inplace=True)
            df_single_nav = df_single_nav.loc[df_single_nav['quote_date'] > before_date, :]

            if df_funds_nav.empty:
                df_funds_nav = df_single_nav
            else:
                df_funds_nav = pd.DataFrame.merge(df_funds_nav, df_single_nav, how='outer',
                                                  on=['quote_date', 'quote_date'])

        df_funds_nav.to_csv('nav_chg.csv', header=True, sep=',')

    def readFundsDataFromCSV(self, path):
        df_funds_nav = pd.read_csv(path, index_col=0)
        df_funds_nav.fillna(0.0, inplace=True)
        df_funds_nav.replace(to_replace=r'\s+', value=0.0, regex=True, inplace=True)
        df_funds_nav.index = df_funds_nav.index.astype(np.datetime64)
        df_funds_nav = df_funds_nav.iloc[:, :].astype(np.float64)
        # print df_funds_nav
        return df_funds_nav

    def readIndicesFromCSV(self, beg_date, path):
        df_indices = pd.read_csv(path, ).sort_values('quote_date', ascending=False)
        df_indices.set_index('quote_date', inplace=True)
        # beg_date = datetime.strptime(beg_date, '%Y-%m-%d')
        df_indices = df_indices.iloc[df_indices.index >= beg_date]
        # print df_indices
        return df_indices

    def fundsCorr(self, df_funds_nav):
        df_corr = df_funds_nav.corr()
        return df_corr

    def fundsStatistics(self, data, path=None):
        df = pd.DataFrame()
        df['mean'] = data.mean()
        df['std'] = data.std()
        df['median'] = data.median()
        df['max'] = data.max()
        df['min'] = data.min()
        df['mad'] = data.mad()
        df['sum'] = data.sum()
        df = df.sort_values(['mean'], ascending=False)
        if path is not None:
            self.toPickles(df, path)
        return df

    def filterCorr(self, df_corr, high, low, sort=False):
        '''
        Base on Conditions to find out needed funds
        :param df_corr:
        :param high: values <= high
        :param low: values >= low
        :return: return dictionary when sort = False
                :return sorted tuples when sort = True
        '''
        pair = {}
        df = df_corr[(df_corr[[col for col in df_corr.columns]] >= low) &
                     ((df_corr[[col for col in df_corr.columns]] <= high) ) ]

        for col in df.columns:
            pair[col] = df.index[df[col].notna()].tolist()

        self.toPickles(pair, 'selected_corr_pair')
        if sort:
            for key, values in pair.iteritems():
                pair[key] = len(values)

            sorted_pair = sorted(pair.items(), key=lambda item:item[1], reverse=True)
            return sorted_pair
        return pair

    def getDedicateFunds(self, df_list, fund_codes):
        if type(fund_codes) == 'list':
            fund_codes = str(fund_codes)

        return df_list[fund_codes]

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
        heatmap.set_clim(0.3, 1)
        plt.tight_layout()
        plt.show()

    def plotFunds(self, df_funds_nav):
        # x_d = df_funds_nav['quote_date']
        # y_d = df_funds_nav['000003_Nav']
        # plt.plot(x_d, y_d, color='r', linewidth=2.0)
        df_funds_nav.plot(x='quote_date', y=[y for y in df_funds_nav.iloc[:, 1:10]], figsize=(100, 500))
        plt.show()

    def toPickles(self, data, path):
        with open(path, 'wb') as f:
            pickle.dump(data, f)

    def mergeDFs(self, left, right, on=None, how='outer', left_index=False, right_index=False, sort=False):
        df = left.merge(right, on=on, how=how, left_index=left_index, right_index=right_index, sort=sort)
        df.sort_index(inplace=True, ascending=False)
        return df

    def getFillNa(self, df, method='ffill'):
        df.fillna(method='ffill', inplace=True)
        return df

    def getRollingMean(self, df, windows=[7, 30]):

        df_1 = pd.DataFrame()
        df.sort_index(inplace=True, ascending=True)
        for window in windows:
            for column in df.columns:
                df_1["{}_{}".format(str.strip(column), window)] = df[column].rolling(window=window,
                                                                                     min_periods=5).mean()
        df = df.join(df_1)

        df.sort_index(inplace=True, ascending=False)
        return df

    def getNormalization(self, df, method='mv'):
        from sklearn import preprocessing
        if method == 'min-max':
            scaler = preprocessing.MinMaxScaler()
        else:
            scaler = preprocessing.StandardScaler()

        df = self.getFillNa(df)
        df = df.replace(np.inf, 0.0)
        # print df.tail(10)
        array = df.iloc[:, :].values.astype(np.float32)
        print array
        scaled = scaler.fit_transform(array)
        df_1 = pd.DataFrame(scaled)
        df_1.columns = df.columns
        df_1.index = df.index
        return df_1


def main():
    beg_date = '2015-01-01'
    fa = C_Fund_Analysis()
    #fa.loadFundsCumNavInCSV(beg_date, 'basic_filtered.ticker')

    df_nav = fa.readFundsDataFromCSV('fund_cum_nav.csv')
    # df_sta = fa.fundsStatistics(df_nav, path='fund_cum_nav_statisic.ticker')
    # df_corr = fa.fundsCorr(df_nav)

    df_filtered = fa.getDedicateFunds(df_nav, ['240020_Nav', '002001_Nav','460005_Nav'])
    #df_sta = fa.fundsStatistics(df_filtered)
    #print df_sta
    #df_corr = fa.fundsCorr(df_filtered)
    # fa.plotCorrHeadMap(df_corr)
    #print df_filtered

    df_indices = fa.readIndicesFromCSV(beg_date, 'indices_data.csv')
    # print df_indices

    df_combined = fa.mergeDFs(left=df_filtered, right=df_indices, left_index=True, right_index=True, how='left')
    df_combined = fa.getFillNa(df_combined)
    df_roll_meaned = fa.getRollingMean(df_combined)
    # print df_roll_meaned
    df_scalered = fa.getNormalization(df_roll_meaned)
    #print df_scalered['240020_Nav'].head(20), df_roll_meaned['240020_Nav'].head(20)


if __name__ == "__main__":
    main()
