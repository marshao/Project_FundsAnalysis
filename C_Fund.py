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

class Fund():

    Fund_Type = '1'


    def __init__(self, fund_code = None, fund_name = None, start_date = None, end_date = None):
        db_server = db.MySQLServer()
        self.session = db_server.getSession()
        self.tb_FundNetValue = db_server.getTable('tb_FundNetValue')
        self.tb_FundManagerHistory = db_server.getTable('tb_FundManagerHistory')
        self.tb_FundInfo = db_server.getTable('tb_FundInfo')

        if fund_code is None and fund_name is None:
            print "fund_name and fund_code should not all be none "
        elif fund_code is None:
            self.fund_name = fund_name
            self.fund_code = self.__getFundCode(fund_name)
        else:
            self.fund_code = fund_code
            self.fund_name = self.__getFundName(fund_code)

        self.fund_type = ''
        self.fund_size = ''
        self.start_date = ''
        self.fund_managed_history = {}
        self.fund_manager_codes = []
        self.fund_manager_names = []
        self.long_status = 1
        self.short_status = 1
        self.unit_net_value = []
        self.cum_net_value = []
        self.daily_chg_rate = []

    def __getFundName(self, fund_code):
        table = self.tb_FundInfo
        stat = select([table.c.fund_name]).where(table.c.fund_code == fund_code)
        #fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0].encode('utf-8')


    def __getFundCode(self, fund_name):
        table = self.tb_FundInfo
        stat = select([table.c.fund_code]).where(table.c.fund_name == fund_name.decode('utf-8'))
        # fund = self.session.query(table).filter_by(fund_code = fund_code)
        result = self.session.execute(stat).fetchall()
        return result[0][0]


def main():
    #fund = Fund(fund_code='004477')
    fund = Fund(fund_name= '嘉实沪港深回报混合型证券投资基金')
    print fund.fund_code


if __name__ == "__main__":
    main()