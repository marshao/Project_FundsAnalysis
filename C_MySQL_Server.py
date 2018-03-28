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
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, MetaData, exists, update, and_, select, bindparam

class MySQLServer():

    def __init__(self, des=None):
        if des is None:
            des = 'pro'
        if des == 'pro':
            self.db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150),
        else:
            self.db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.176.50.233/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150)

        DBSession = sessionmaker(bind=self.db_engine)
        self.session = DBSession()
        self.meta = MetaData(self.db_engine)

        # Delare Table
        self.fund_list = Table('tb_FundList', meta, autoload=True)
        #SHFactors = Table('tb_StockSHFactors', meta, autoload=True)

    def getEngine(self, des=None):
        return self.db_engine

    def getTable(self, tb_name):
        return  Table(tb_name, self.meta, autoload=True)

    def processData(self, func=None, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        if func == 'insert':
            self.__insertData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'update':
            self.__updateData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'select':
            self.__selectData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table,
                              src_table=src_table, parameter=parameter)
        elif func == 'trunk':
            self.__trunkData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'upsert':
            self.__upsertData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        else:
            print "DB engine do not have %s function"%func


    def __insertData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        pass

    def __upsertData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        stat = des_table.insert(). \
            values(fund_code=bindparam('fund_code'), fund_name = bindparam('fund_name'), \
                   long_status=bindparam('long_status'), short_status = bindparam('short_status')). \
                    where(~exists(self.__updateData()))

    def __updateData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        pass

    def __selectData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        pass

    def __trunkData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        pass