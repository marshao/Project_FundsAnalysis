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
from sqlalchemy.dialects.mysql import insert
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


class MySQLServer():
    db_engine = create_engine('mysql+mysqldb://marshao:123@localhost/DB_FundsAnalysis?charset=utf8',
                              encoding='utf-8', pool_size=150, echo=False)
    # db_engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
    #                          encoding='utf-8', pool_size=150, echo=False)
    DBSession = sessionmaker(bind=db_engine)
    session = DBSession()
    # session.execute('show databases')
    meta = MetaData(db_engine)

    def __init__(self, des=None):
        if des is None:
            des = 'pro'
        if des == 'pro':
            pass
        else:
            pass
            '''
            self.db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.176.50.233/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150)
            
        
        self.db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150, echo=True)
            '''
        self.db_engine = create_engine(
            'mysql+mysqldb://marshao:123@localhost/DB_FundsAnalysis?charset=utf8',
            encoding='utf-8', pool_size=150, echo=True)
        #DBSession = sessionmaker(bind=self.db_engine)
        # session = DBSession()
        # session.execute('show databases')
        #self.meta = MetaData(self.db_engine)


        # Delare Table
        # SHFactors = Table('tb_StockSHFactors', self.meta, autoload=True)

    def getEngine(self, des=None):
        return MySQLServer.db_engine

    def getSession(self):
        return MySQLServer.session

    def getTable(self, tb_name):
        return Table(tb_name, MySQLServer.meta, autoload=True)


    def processData(self, func=None, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        if func == 'insert':
            self.__insertData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'update':
            self.__updateData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'select':
            self.__selectData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table,
                              src_table=src_table, parameter=parameter)
        elif func == 'truncate':
            self.__trunkData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'upsert':
            self.__upsertData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table, src_table=src_table, parameter=parameter)
        elif func == 'delete':
            self.__deleteData(fund_code=fund_code, quote_time=quote_time, sql_script=sql_script, des_table=des_table,
                              src_table=src_table, parameter=parameter)
        else:
            print "DB engine do not have %s function"%func

        MySQLServer.session.commit()
        MySQLServer.session.close()


    def __insertData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        if parameter is not None:
            MySQLServer.session.execute(sql_script, parameter)
        else:
            if type(sql_script) == 'list':
                for each_sql in sql_script:
                    MySQLServer.session.execute(each_sql)
            else:
                MySQLServer.session.execute(sql_script)
        MySQLServer.session.commit

    def __upsertData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        if (sql_script is not None) and (parameter is not None):
            MySQLServer.session.execute(sql_script, parameter)
        elif (type(sql_script) != 'str') and (parameter is None):
            for each_sql in sql_script:
                # print each_sql
                MySQLServer.session.execute(each_sql)
        else:
            MySQLServer.session.execute(sql_script)
        MySQLServer.session.commit


    def __updateData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        stat = des_table.update(). \
            values(fund_code=bindparam('fund_code'), fund_name=bindparam('fund_name'), \
                   long_status=bindparam('long_status'), short_status=bindparam('short_status'))

    def __selectData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        pass

    def __trunkData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        stat = 'Truncate table %s;' % des_table
        MySQLServer.session.execute(stat)

    def __deleteData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None,
                     parameter=None):
        MySQLServer.session.execute(sql_script)

    def buildQuery(self, func, parameters, des_table_name, ):
        insert_stats = []
        upsert_stats = []
        for parameter in parameters:
            params = {k: v for k, v in parameter.items() if v is not None}

            cols = ','.join([key for key, value in params.items()])

            vals = '{}'.format(
                ','.join([str(value) if isinstance(value, float) else value for key, value in params.items()]))

            up_vals = ','.join('{} = {}'.format(key, str(value)) for (key, value) in params.items())

            insert_stat = "insert into {} ({}) VALUES ({})".format(des_table_name, cols, vals)
            upsert_stat = "insert into {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(des_table_name,
                                                                                              cols, vals, up_vals)
            insert_stats.append(insert_stat)
            upsert_stats.append(upsert_stat)

        if func == 'upsert':
            #print upsert_stat
            return upsert_stats
        else:
            return insert_stats
