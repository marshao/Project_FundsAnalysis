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
from sqlalchemy import select
import pandas as pd
from datetime import *

class Fund():
    def __init__(self):
        db_server = db.MySQLServer()
        self.engine = db_server.getEngine()
        self.session = db_server.getSession()
        self.tb_FundNetValue = db_server.getTable('tb_FundNetValue')
        self.tb_FundManagerHistory = db_server.getTable('tb_FundManagerHistory')
        self.tb_FundInfo = db_server.getTable('tb_FundInfo')
        self.tb_FundList = db_server.getTable('tb_FundList')


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
            unit_net_value_list = pd.read_sql(stat, con=self.engine, index_col='quote_date')
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

        current_year = datetime.today().year
        df_result = df_list.loc[(df_list['issue_date'].apply(lambda x: x.year) - current_year) >= year]

        return df_result




def main():
    #fund = FundInstance(fund_code='003503')
    # fund = Fund(fund_name= '嘉实沪港深回报混合型证券投资基金')
    fund_list = FundList()
    funds = fund_list.getBuyableFunds()
    print fund_list.fund_types['2']
    # print fund_list.getFundsInType(fund_type_index=1, df_list=funds)
    # print fund_list.getFundsIssuedBeforeThan(df_list=funds, datestr='20100101')
    print fund_list.getFundsIssuedLongerThan(df_list=funds, year=1)


if __name__ == "__main__":
    main()