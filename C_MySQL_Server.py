# -*- coding:utf-8 -*-
#!/usr/local/bin/python
############################################################################
'''''
# 程序：Create a mysql object to execute all mysql operations
# 功能：抓取东方财富网上基金相关数据
# 创建时间：2018/03/19
# 作者：Guan Hao
'''
#############################################################################

from sqlalchemy import create_engine

class MySQLServer():

    def __init__(self, des=None):
        if des is None:
            des = 'pro'
        if des == 'pro':
            self.pro_db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150),
        else:
            self.dev_db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.176.50.233/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150)

    def getEngine(self, des=None):
        if des is None:
            des = 'pro'

        if des == 'pro':
            return self.pro_db_engine
        else: return self.dev_db_engine

    def processData(self, func=None, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None):
        if func == 'insert':
            self.__insertData(und_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None)
        elif func == 'update':
            self.__udpateData(und_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None)
        elif func == 'select':
            self.__selectData(und_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None)
        elif func == 'trunk':
            self.__trunkData(und_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None)
        else:
            print "DB engine do not have %s function"%func


    def __insertData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None):
        pass

    def __updateData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None):
        pass

    def __selectData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None):
        pass

    def __trunkData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None):
        pass