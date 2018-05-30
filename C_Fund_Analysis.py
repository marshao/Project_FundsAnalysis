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
import random
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
        df_funds_nav.fillna(method='pad', inplace=True)
        df_funds_nav.replace(to_replace=r'\s+', value=0.0, regex=True, inplace=True)
        df_funds_nav.index = df_funds_nav.index.astype(np.datetime64)
        df_funds_nav = df_funds_nav.iloc[:, :].astype(np.float64)
        # print df_funds_nav
        return df_funds_nav

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

    def getFillNa(self, df, method='ffill'):
        df.fillna(method=method, inplace=True)
        return df


class C_Fund_Data_PreProcession():
    def readIndicesFromCSV(self, beg_date, path):
        df_indices = pd.read_csv(path, ).sort_values('quote_date', ascending=False)
        df_indices.fillna(method='pad', inplace=True)
        df_indices.set_index('quote_date', inplace=True)
        df_indices = df_indices.iloc[df_indices.index >= beg_date]
        # df_indices.replace(' ', 0.0)
        # print df_indices.head(10)
        # print len(df_indices.iloc[0:1, -2:-1][0])
        df_indices = df_indices.iloc[:, :].astype(np.float32)
        # print df_indices
        return df_indices

    def mergeDFs(self, left, right, on=None, how='outer', left_index=False, right_index=False, sort=False):
        df = left.merge(right, on=on, how=how, left_index=left_index, right_index=right_index, sort=sort)
        df.fillna(inplace=True, method='pad')
        df.sort_index(inplace=True, ascending=False)
        return df

    def getFillNa(self, df, method='ffill'):
        df.fillna(method=method, inplace=True)
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
        '''
        if method == 'min-max':
            scaler = preprocessing.MinMaxScaler()
        else:
            scaler = preprocessing.StandardScaler()

        from sklearn import preprocessing
        df = self.getFillNa(df)
        df = df.replace(np.inf, 0.0)
        # print df.tail(10)
        array = df.iloc[:, :].values.astype(np.float32)
        scaled = scaler.fit_transform(array)
        df_1 = pd.DataFrame(scaled)
        df_1.columns = df.columns
        df_1.index = df.index
        '''
        df_1 = pd.DataFrame()
        df = self.getFillNa(df, method='pad')
        for column in df:
            # print column, df[column].mean(), df[column].std()
            df_1[column] = (df[column] - df[column].mean()) / df[column].std()

        return df_1

    def getOutlier(self, df):
        for column in df:
            avg = df[column].mean()
            std = df[column].std()
            std_3 = 3. * std
            std_n_3 = -3. * std
            # print column, avg , std, std_3, std_n_3
            df[column] = [std_3 if (x - avg) > std_3 else x for x in df[column]]
            df[column] = [std_n_3 if (x - avg) < std_n_3 else x for x in df[column]]
        # print df - df_1
        return df

    def getDailyLogIncrease(self, df, funds):
        df.sort_index(inplace=True, ascending=True)
        for column in df[funds]:
            df['{}_{}'.format(column, 'linc')] = np.log(df[column]).pct_change()
        df.fillna(0.0, inplace=True)
        df.sort_index(inplace=True, ascending=False)
        return df

    def getSamplesDegroupedByWeek(self, df, funds):
        '''
        Degrouped by calendar week
        :param df:
        :param period:
        :return:
        '''
        samples_sets = []
        fund_samples = []
        indexes = []

        b_y = df.index[0].isocalendar()[0]
        b_w = df.index[0].isocalendar()[1]
        #print b_y, b_w
        for date in df.index:
            year = date.isocalendar()[0]
            week = date.isocalendar()[1]
            if (b_y == year) and (b_w == week):
                indexes.append(date)
            else:
                # df_sample = df.loc[df.index.isin(indexes)]
                samples_sets.append(df.loc[df.index.isin(indexes)])
                indexes = []
                b_y, b_w = year, week
                indexes.append(date)
        return samples_sets

    def getDegroupedSampleByRolling(self, df, funds, period=5):
        '''
        Degrouped by rolling period
        sample group , if t = now.date then first gourp is (t-period to t- 2*period)
        labels group, if t = now.date then frist lable is sum(t to t-period+5)
        :param df:
        :param period:
        :return:
        '''
        sample_sets = []
        count = df.shape[0]
        # print b_y, b_w
        for i in range(period * 2, count + 1):
            df_t = df.iloc[i - period:i, :]
            sample_sets.append(df_t)
        return sample_sets

    def getDegroupedLabelVectors3levelsByRolling(self, df, funds, up, low, period=5):
        label_sets = []
        u_c = 0
        l_c = 0
        c = 0
        count = df.shape[0]
        # print 'up:{}, low:{}'.format(up, low)
        for fund in funds:
            labels = []
            for i in range(0, (count + 1 - 2 * period)):
                w_inc = df['{}_{}'.format(fund, 'linc')][i:i + period].sum()
                if (w_inc > up):
                    label = [1, 0, 0]
                    u_c += 1
                elif w_inc < low:
                    label = [0, 0, 1]
                    l_c += 1
                else:
                    label = [0, 1, 0]
                    c += 1
                labels.append(label)

            label_sets.append(labels)
            label_sets.append(
                ["fund: {}, Boundary: {}, Up: {},  Stay: {}, Low:{}".format(fund, (up, low), u_c, c, l_c)])

        return label_sets

    def getLabelVectors5Levels(self, sample_sets, funds, up, down):
        label_sets = []
        up = up
        up_2 = 3 * up
        low = down
        low_2 = 3 *low
        u_c = 0
        u_c_2 = 0
        l_c = 0
        l_c_2 = 0
        c = 0
        print 'up2: {}, up:{}, low:{}, low2:{}'.format(up_2, up, low, low_2)
        for fund in funds:
            labels = []
            for sample in sample_sets:
                w_inc = sample['{}_{}'.format(fund, 'linc')].sum()
                print w_inc
                if w_inc > up_2:
                    label = [1, 0, 0, 0, 0]
                    u_c_2 += 1
                elif (w_inc <= up_2) and (w_inc > up):
                    label = [0, 1, 0, 0, 0]
                    u_c += 1
                elif w_inc < low_2:
                    label = [0, 0, 0, 0, 1]
                    l_c_2 += 1
                elif (w_inc >= low_2) and (w_inc < low):
                    label = [0, 0, 0, 1, 0]
                    l_c += 1
                else:
                    label = [0, 0, 1, 0, 0]
                    c += 1
                labels.append(label)

            label_sets.append(labels)
            label_sets.append(
                ["fund: {}, Boundary: {}, Up2: {}, Up: {},  Stay: {}, Low:{}, Low2:{}".format(fund, (up, down), u_c_2,
                                                                                              u_c, c, l_c, l_c_2)])
        return label_sets

    def getLabelVectors3levels(self, sample_sets, funds, up, low):
        label_sets = []
        u_c = 0
        l_c = 0
        c = 0
        # print 'up:{}, low:{}'.format(up, low)
        for fund in funds:
            labels = []
            for sample in sample_sets:
                w_inc = sample['{}_{}'.format(fund, 'linc')].sum()
                if (w_inc > up):
                    label = [1, 0, 0]
                    u_c += 1
                elif w_inc < low:
                    label = [0, 0, 1]
                    l_c += 1
                else:
                    label = [0, 1, 0]
                    c += 1
                labels.append(label)

            label_sets.append(labels)
            label_sets.append(
                ["fund: {}, Boundary: {}, Up: {},  Stay: {}, Low:{}".format(fund, (up, low), u_c, c, l_c)])
        return label_sets

    def getDataSets(self, sample_sets, label_sets, cv_por, test_por):
        train_sets, cross_validation_sets, test_sets = {}, {}, {}
        # Matching the t period features to t+1 period label
        # by pop out the first feature element and last label element
        sample_sets.pop(0)
        label_sets = label_sets[0]
        label_sets.pop()

        # Shuffle sample_sets and label_sets
        sample_sets, label_sets, idx_order = self.__shuffle_lists(sample_sets, label_sets)

        # Getting CV samples:
        cross_validation_sets['sample_sets'], rest_sample_sets = self.__getSubSets(cv_por, sample_sets)
        cross_validation_sets['label_sets'], rest_label_sets = self.__getSubSets(cv_por, label_sets)

        # Getting testing and training samples:
        test_sets['sample_sets'], train_sets['sample_sets'] = self.__getSubSets(test_por, rest_sample_sets)
        test_sets['label_sets'], train_sets['label_sets'] = self.__getSubSets(test_por, rest_label_sets)

        return train_sets, cross_validation_sets, test_sets

    def __shuffle_lists(self, sample, label):
        l1, l2 = [], []
        idx = range(len(sample))
        random.seed(1)
        random.shuffle(idx)
        for i in idx:
            l1.append(sample[i])
            l2.append(label[i])
        return l1, l2, idx

    def __getSubSets(self, por, list):
        pos = int(len(list) * por)
        return list[0:pos], list[pos:]

    def writeIntoCSV(self, df, path):
        df.to_csv(path, header=True, sep=',', index=0)


def fund_Analysis(beg_date, funds):
    fa = C_Fund_Analysis()
    # fa.loadFundsCumNavInCSV(beg_date, 'basic_filtered.ticker')

    df_nav = fa.readFundsDataFromCSV('fund_cum_nav.csv')
    df_filtered = fa.getDedicateFunds(df_nav, funds)
    '''
    # Statistic Analysis
    df_sta = fa.fundsStatistics(df_filtered)
    print df_sta
    df_corr = fa.fundsCorr(df_filtered)
    fa.plotCorrHeadMap(df_corr)
    print df_filtered
    '''
    return df_filtered


def fund_data_proprocessing(beg_date, funds, df_filtered, degroup='Roll'):
    period = 5
    dpp = C_Fund_Data_PreProcession()
    df_indices = dpp.readIndicesFromCSV(beg_date, 'indices_data.csv')
    df_combined = dpp.mergeDFs(left=df_filtered, right=df_indices, left_index=True, right_index=True, how='left')
    df_roll_meaned = dpp.getRollingMean(df_combined)
    df_linc = dpp.getDailyLogIncrease(df_roll_meaned, funds)
    df_normalized = dpp.getNormalization(df_linc)
    df_outlierd = dpp.getOutlier(df_normalized)
    if degroup == 'Roll':
        # NN Period Rolling Degoup
        sample_sets = dpp.getDegroupedSampleByRolling(df_outlierd, funds, period=period)
        label_sets = dpp.getDegroupedLabelVectors3levelsByRolling(df_outlierd, funds, up=0.9, low=-0.6, period=period)
    elif degroup == 'Week':
        # Calendar Week Degroup
        sample_sets = dpp.getSamplesDegroupedByWeek(df_outlierd, funds)
        label_sets = dpp.getLabelVectors3levels(sample_sets, funds, up=0.6, low=-0.6)

    train_sets, cv_sets, test_sets = dpp.getDataSets(sample_sets, label_sets, cv_por=0.15, test_por=0.15)
    return train_sets, cv_sets, test_sets

def main():
    beg_date = '2015-01-01'
    # funds = ['240020_Nav', '002001_Nav','460005_Nav']
    # funds = ['240020_Nav']
    funds = ['002001_Nav']
    #funds = ['460005_Nav']
    df_filtered = fund_Analysis(beg_date, funds)
    fund_data_proprocessing(beg_date, funds, df_filtered)
    train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered)
    #print len(cv_sets['sample_sets']), len(train_sets['sample_sets'])




if __name__ == "__main__":
    main()
