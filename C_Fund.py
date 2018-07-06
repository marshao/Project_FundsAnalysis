# -*- coding:utf-8 -*-
#!/usr/local/bin/python
############################################################################
'''''
# 程序： Build Fund Class
# 功能：Build Eeah Fund as an object, and provide according functions.
# 创建时间：2018/04/11
# 更新历史：
#
# 使用库：requests、BeautifulSoup4、pymysql,pandas
# 作者：Guan Hao
'''
#############################################################################

import C_MySQL_Server as db
from sqlalchemy import select, func, and_
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pickle

class Fund():
    def __init__(self):
        db_server = db.MySQLServer()
        self.engine = db_server.getEngine()
        self.session = db_server.getSession()
        self.tb_FundNetValue = db_server.getTable('tb_FundNetValue')
        self.tb_FundManagerHistory = db_server.getTable('tb_FundManagerHistory')
        self.tb_FundInfo = db_server.getTable('tb_FundInfo')
        self.tb_FundList = db_server.getTable('tb_FundList')
        self.tb_FundPeriodicIncreaseDetail = db_server.getTable('tb_FundPeriodicIncreaseDetail')
        self.tb_FundCumIncomeRate_1M = db_server.getTable('tb_FundCumIncomeRate_1M')
        self.tb_FundCumIncomeRate_3M = db_server.getTable('tb_FundCumIncomeRate_3M')
        self.tb_FundCumIncomeRate_6M = db_server.getTable('tb_FundCumIncomeRate_6M')
        self.tb_FundCumIncomeRate_1Y = db_server.getTable('tb_FundCumIncomeRate_1Y')
        self.tb_FundCumIncomeRate_3Y = db_server.getTable('tb_FundCumIncomeRate_3Y')
        self.tb_FundCumIncomeRate_5Y = db_server.getTable('tb_FundCumIncomeRate_5Y')
        self.tb_FundCumIncomeRate_All = db_server.getTable('tb_FundCumIncomeRate_All')


class FundInstance(Fund):
    '''
    Fund Class stands for the individual instance of each fund.
    '''

    Fund_Type = '1'


    def __init__(self, fund_code = None, fund_name = None, start_date = None, end_date = None):
        Fund.__init__(self)
        if fund_code is None and fund_name is None:
            print "fund_name and fund_code should not all be none "
        elif fund_code is None:
            self.fund_name = fund_name
            self.fund_code = self.__getFundCode(fund_name)
        else:
            self.fund_code = fund_code
            self.fund_name = self.__getFundName(fund_code)

        self.fund_type = self.__getFundType(self.fund_code)
        self.up_to_date_size = self.__getUnits(self.fund_code)
        self.up_to_date_value = self.__getAssetValue(self.fund_code)
        self.issue_date = self.__getEstablishDate(self.fund_code)
        self.issue_size = self.__getEstablishScale(self.fund_code)
        self.fund_managed_history = {}
        self.fund_manager_codes = self.__getFundManagerCodes(self.fund_code)
        self.fund_manager_names = self.__getFundManagerNames(self.fund_code)
        self.long_status = self.__getLongStatus(self.fund_code)
        self.short_status = self.__getShortStatus(self.fund_code)
        self.buy_fee = self.__getBuyFee(self.fund_code)
        self.sale_fee = self.__getSaleFee(self.fund_code)
        self.bench_mark = self.__getBenchMark(self.fund_code)


    def __getFundName(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.fund_name]).where(table.c.fund_code == fund_code)
        #fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getFundCode(self, fund_name):
        table = self.tb_FundInfo
        stat = select([table.c.fund_code]).where(table.c.fund_name == fund_name.decode('utf-8'))
        # fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getFundType(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.fund_type]).where(table.c.fund_code == fund_code)
        # fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getUnits(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.units]).where(table.c.fund_code == fund_code)
        # fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getAssetValue(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.asset_value]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getBuyFee(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.buy_fee]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getSaleFee(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.sale_fee]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getBenchMark(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.benchmark]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getEstablishDate(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.establish_date]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getEstablishScale(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.establish_scale]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getFundManagerNames(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.fund_manager_name_1,
                       table.c.fund_manager_name_2,
                       table.c.fund_manager_name_3,
                       ]).where(table.c.fund_code == fund_code)
        # fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0]

    def __getFundManagerCodes(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.fund_manager_code_1,
                       table.c.fund_manager_code_2,
                       table.c.fund_manager_code_3
                       ]).where(table.c.fund_code == fund_code)
        # fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0]

    def __getLongStatus(self, fund_code):
        table = self.tb_FundList
        stat = select([table.c.long_status]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def __getShortStatus(self, fund_code):
        table = self.tb_FundList
        stat = select([table.c.short_status]).where(table.c.fund_code == fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]

    def getUnitNetValueList(self, fund_code):
        stat = "select * from tb_FundNetValue where fund_code = %s order by quote_date DESC" % fund_code
        try:
            unit_net_value_list = pd.read_sql(stat, con=self.engine)
        except Exception as e:
            print (fund_code, e)
        return unit_net_value_list


class FundList(Fund):
    '''
    FundList is the class stands for a collection of funds those match some requirements.
    '''

    def __init__(self):
        Fund.__init__(self)
        self.full_list = self.__getFullList()
        self.fund_types = self.__getFundTypes(self.full_list)

    def __getFullList(self):
        try:
            sql_select_all_fundInfo = 'select * from tb_FundInfo'
            sql_select_all_fundList = 'select fund_code, long_status, short_status  from tb_FundList'
            df_fundInfo = pd.read_sql(sql=sql_select_all_fundInfo, con=self.engine)
            df_fundList = pd.read_sql(sql=sql_select_all_fundList, con=self.engine)
            df_result = pd.merge(df_fundList, df_fundInfo, how='inner', on=['fund_code'])
            df_result.set_index('fund_code', inplace=True)
            # '''
            df_result = df_result.loc[:, ['fund_name', 'long_status', 'short_status', 'fund_type',
                                          'issue_date', 'establish_sacle', 'asset_value', 'sale_fee', 'buy_fee',
                                          'buy_fee2', 'mgt_fee', 'total_div', 'fund_manager_code_1',
                                          'fund_manager_name_1',
                                          'fund_manager_code_2', 'fund_manager_name_2']]
        except Exception as e:
            print ("Fund list retrieving error", e)

        return df_result

    def getBuyableFunds(self, fund_list=None):
        if fund_list is None:
            fund_list = self.full_list
        df_result = fund_list.loc[fund_list['long_status'] == '1']
        return df_result

    def getSaleableFunds(self, fund_list=None):
        if fund_list is None:
            fund_list = self.full_list
        df_result = fund_list.loc[fund_list['short_status'] == '1']
        return df_result

    def getTradeableFunds(self, fund_list=None):
        if fund_list is None:
            fund_list = self.full_list
        df_result = fund_list.loc[(fund_list['short_status'] == '1') & (fund_list['long_status'] == '1')]
        return df_result

    def __getFundTypes(self, fund_list=None):
        if fund_list is None:
            fund_list = self.full_list
        fund_types = {}
        types = fund_list['fund_type'].unique()
        for i in range(1, len(types)):
            fund_types[str(i)] = types[i - 1]
        return fund_types

    def getFundsInType(self, df_list=None, fund_type_index=1):
        if df_list is None:
            df_list = self.full_list
        type = self.fund_types[str(fund_type_index)]
        df_result = df_list.loc[df_list['fund_type'] == type]
        return df_result

    def getFundsIssuedBeforeThan(self, df_list=None, datestr=None):
        if df_list is None:
            df_list = self.full_list
        if datestr is None:
            datestr = datetime.today()
        elif len(datestr) == 8:
            y = int(datestr[0:4])
            m = int(datestr[4:6])
            d = int(datestr[6:8])
            datestr = datetime(y, m, d)
        else:
            print ("datestr need a 8 digits string in Chinese format")

        df_result = df_list.loc[df_list['issue_date'] <= datestr]
        return df_result

    def getFundsIssuedLongerThan(self, df_list=None, year=None):
        if df_list is None:
            df_list = self.full_list
        if year is None:
            year = 0

        current = datetime.today()
        target_year = current - timedelta(days=(year * 360))
        df_result = df_list.loc[df_list['issue_date'] <= target_year]
        return df_result

    def filterFundsAssetValue(self, df_list = None, asset_value = 1.0):
        if df_list is None:
            df_list = self.full_list
        return  df_list.loc[df_list['asset_value'] >= asset_value]

    def dumpFundCodes(self, path, df_list=None):
        if df_list is None:
            df_list = self.full_list
        df_list.reset_index(inplace=True)
        # print df_list
        # with open('selected_fund_codes.csv', wb) as f:
        df_list.to_csv(path_or_buf='{}.csv'.format(path), header=False, columns=['fund_code'], index=False)
        df_list['fund_code'].to_pickle('{}.ticker'.format(path))

    def filterFundsCumIncomeRateHeads(self, df_list=None, periods=['1M', '3M', '6M', '1Y', '3Y', '5Y', 'All']):
        '''
        Fetch funds cumlative income rates in different periods [3Y, 1Y, 6M, 3M, 1M] for first 400 funds
        Rank the income rates and calculate sum, mean, std
        The one with small mean and sum should have higher income rates.
        The one with small std shoud have stabler income rates.
        :param df_list:
        :param periods:
        :return: first 30 funds order by sum, mean, std
        '''
        if df_list is None:
            df_list = self.full_list

        fund_codes = df_list.index.tolist()
        table = "tb_FundCumIncomeRate_3Y"
        sess = self.session
        df_cum_rates = pd.DataFrame()
        try:
            fund_codes_str = ", ".join(fund_codes)

            table_list = ["tb_FundCumIncomeRate_3Y", "tb_FundCumIncomeRate_1Y", "tb_FundCumIncomeRate_6M",
                          "tb_FundCumIncomeRate_3M", "tb_FundCumIncomeRate_1M"]

            for table in table_list:
                sql_query = 'select a.fund_code, a.fund_cum_income_rate from {} a ' \
                            'inner join(select fund_code, max(quote_date) as max_date from {} ' \
                            'where fund_code in ({}) group by fund_code) b ' \
                            'on a.fund_code = b.fund_code and a.quote_date = b.max_date ' \
                            'order by a.fund_cum_income_rate DESC;'.format(table, table, fund_codes_str)
                records = pd.DataFrame(sess.execute(sql_query).fetchall())
                col_name = 'cum_rate_{}'.format(table[-2:])
                records.columns = ['fund_code', col_name]
                if df_cum_rates.empty:
                    df_cum_rates = records
                else:
                    df_cum_rates = pd.merge(df_cum_rates, records, on=['fund_code'])
            #print df_cum_rates.sort_values('cum_rate_1Y', ascending=False)
            df_cum_rates['3Y_rank'] = df_cum_rates['cum_rate_3Y'].rank(ascending=False)
            df_cum_rates['1Y_rank'] = df_cum_rates['cum_rate_1Y'].rank(ascending=False)
            df_cum_rates['6M_rank'] = df_cum_rates['cum_rate_6M'].rank(ascending=False)
            df_cum_rates['3M_rank'] = df_cum_rates['cum_rate_3M'].rank(ascending=False)
            df_cum_rates['1M_rank'] = df_cum_rates['cum_rate_1M'].rank(ascending=False)
            df_cum_rank = df_cum_rates[['fund_code', '3Y_rank', '1Y_rank', '6M_rank', '3M_rank', '1M_rank']]
            df_cum_rank.set_index('fund_code', inplace=True)
            df_cum_rank['sum'] = df_cum_rank.sum(axis=1)
            df_cum_rank['mean'] = df_cum_rank.mean(axis=1)
            df_cum_rank['std'] = df_cum_rank.std(axis = 1)
            df_cum_rank = df_cum_rank.sort_values(['mean', 'sum', 'std'], ascending=True)
            #print df_cum_rank
            return df_cum_rank.iloc[0:30, :]
        except Exception as e:
            print "Fucking error again: ", e
        return  df_list


    def filterFundsPeriodicIncrease(self, df_list=None, periodic_increase=None, cls_mark='good',
                                    above_cls_avg=False, above_sh300=False):
        if df_list is None:
            df_list = self.full_list

        table = self.tb_FundPeriodicIncreaseDetail
        sess = self.session
        fund_codes = df_list.index.tolist()
        # print fund_codes
        # print
        records = pd.DataFrame(sess.query(table).filter(table.c.fund_code.in_(fund_codes)).all())

        # Excelent Funds
        if cls_mark == 'excelent':
            excelent_funds = records.loc[(records['this_year_cls_mark'] == 'excelent') &
                                         (records['last_week_cls_mark'] == 'excelent') &
                                         (records['last_month_cls_mark'] == 'excelent') &
                                         (records['last_3_month_cls_mark'] == 'excelent') &
                                         (records['last_6_month_cls_mark'] == 'excelent') &
                                         (records['last_year_cls_mark'] == 'excelent') &
                                         (records['last_2_year_cls_mark'] == 'excelent') &
                                         (records['last_3_year_cls_mark'] == 'excelent')]
            filtered_funds = excelent_funds
            # print excelent_funds.shape[0]
        # Excelent and Good Funds
        elif cls_mark == 'good':
            good_funds = records.loc[
                ((records['this_year_cls_mark'] == 'excelent') | (records['this_year_cls_mark'] == 'good')) &
                ((records['last_week_cls_mark'] == 'excelent') | (records['last_week_cls_mark'] == 'good')) &
                ((records['last_month_cls_mark'] == 'excelent') | (records['last_month_cls_mark'] == 'good')) &
                ((records['last_3_month_cls_mark'] == 'excelent') | (records['last_3_month_cls_mark'] == 'good')) &
                ((records['last_6_month_cls_mark'] == 'excelent') | (records['last_6_month_cls_mark'] == 'good')) &
                ((records['last_year_cls_mark'] == 'excelent') | (records['last_year_cls_mark'] == 'good')) &
                ((records['last_2_year_cls_mark'] == 'excelent') | (records['last_2_year_cls_mark'] == 'good')) &
                ((records['last_3_year_cls_mark'] == 'excelent') | (records['last_3_year_cls_mark'] == 'good'))]
            filtered_funds = good_funds
            # print good_funds.shape[0]

        if above_sh300:
            filtered_funds = filtered_funds.loc[(filtered_funds['this_year_inc'] >= filtered_funds['this_year_sh300']) &
                                                (filtered_funds['last_week_inc'] >= filtered_funds['last_week_sh300']) &
                                                (filtered_funds['last_month_inc'] >= filtered_funds[
                                                    'last_month_sh300']) &
                                                (filtered_funds['last_3_month_inc'] >= filtered_funds[
                                                    'last_3_month_sh300']) &
                                                (filtered_funds['last_6_month_inc'] >= filtered_funds[
                                                    'last_6_month_sh300']) &
                                                (filtered_funds['last_year_inc'] >= filtered_funds['last_year_sh300']) &
                                                (filtered_funds['last_2_year_inc'] >= filtered_funds[
                                                    'last_2_year_sh300']) &
                                                (filtered_funds['last_3_year_inc'] >= filtered_funds[
                                                    'last_3_year_sh300'])]

        fund_codes = filtered_funds['fund_code'].tolist()
        df_result = df_list.loc[df_list.index.isin(fund_codes)]
        # print df_result.shape
        return df_result

    def filterFundsYearQuarterIncrease(self, df_list=None, periodic_increase=None, cls_mark='good',
                                       above_cls_avg=False, above_sh300=False):
        if df_list is None:
            df_list = self.full_list

        table = self.tb_FundYearQuarterIncreaseDetail
        sess = self.session
        fund_codes = df_list.index.tolist()
        # print fund_codes
        # print
        df_records = pd.DataFrame(sess.query(table).filter(table.c.fund_code.in_(fund_codes)).all())

        return df_records

    def filterFundManagerHistory(self, df_list=None):
        if df_list is None:
            df_list = self.full_list

        table = self.tb_FundManagerHistory
        sess = self.session

        fund_manager_codes = df_list['fund_manager_code_1'].tolist()
        fund_manager_codes = list(set(fund_manager_codes))
        df_records = pd.DataFrame(sess.query(table).filter(table.c.manager_code.in_(fund_manager_codes)).all())

        df_records['return_rate'] = df_records['return_rate'].astype(np.float32)
        df_records['class_average_return'] = df_records['class_average_return'].astype(
            np.float32)
        # Find whether managers' funds run better than class average.
        df_records['win_rate'] = (df_records['return_rate'] >= df_records['class_average_return']).astype(np.float32)

        # Aggregate informations of manager's records, compute the ratio of win records
        df_agg = df_records.groupby(by=['manager_code']).agg({'fund_code': 'count',
                                                              'return_rate': ['mean', 'max'],
                                                              'class_average_return': ['mean', 'max'],
                                                              'win_rate': lambda x: sum(x) / x.count()}) \
            .sort_values([('win_rate', '<lambda>')], ascending=False)

        # Find managers whose win_rates are better than 50%
        df_agg = df_agg.loc[df_agg[('win_rate', '<lambda>')] > 0.5]

        # Find managers whose win_rates is > 0.5 and running more funds
        manager_codes = df_agg.head(200).sort_values([('fund_code', 'count')], ascending=False).index.tolist()

        # Sort funds run by better manager
        df_list = df_list.loc[df_list['fund_manager_code_1'].isin(manager_codes)]

        return df_list


def funds_basic_sort():
    fund_list = FundList()
    funds = fund_list.getBuyableFunds()
    # funds = fund_list.getFundsInType(funds, 1)
    funds = fund_list.getFundsIssuedBeforeThan(df_list=funds, datestr='20150101')
    funds = fund_list.filterFundsAssetValue(df_list=funds, asset_value=5.0)
    print "Fund Asset Value filtered {}".format(funds.shape[0])
    funds = fund_list.filterFundManagerHistory(df_list=funds)
    print "Fund Managers filtered {}".format(funds.shape[0])
    funds = fund_list.filterFundsCumIncomeRateHeads(df_list=funds)
    print "Fund Cumulative Income Rate filtered {}".format(funds.shape[0])

    funds = fund_list.filterFundsPeriodicIncrease(df_list=funds, cls_mark='good', above_sh300=True)
    print "Fund Periodic Increase filtered {}".format(funds.shape[0])

    fund_list.dumpFundCodes('basic_filtered', funds)

def main():
    funds_basic_sort()

if __name__ == "__main__":
    main()
