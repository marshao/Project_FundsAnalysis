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

class Fund():

    Fund_Type = '1'


    def __init__(self, fund_code = None, fund_name = None, start_date = None, end_date = None):
        db_server = db.MySQLServer()
        self.engine = db_server.getEngine()
        self.session = db_server.getSession()
        self.tb_FundNetValue = db_server.getTable('tb_FundNetValue')
        self.tb_FundManagerHistory = db_server.getTable('tb_FundManagerHistory')
        self.tb_FundInfo = db_server.getTable('tb_FundInfo')
        self.tb_FundList = db_server.getTable('tb_FundList')

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


def main():
    fund = Fund(fund_code='003503')
    # fund = Fund(fund_name= '嘉实沪港深回报混合型证券投资基金')
    print fund.fund_name
    print fund.getUnitNetValueList('003503')


if __name__ == "__main__":
    main()