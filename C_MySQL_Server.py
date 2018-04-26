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


class MySQLServer():

    db_engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
                              encoding='utf-8', pool_size=150, echo=False)
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
            '''
        self.db_engine = create_engine(
            'mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
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
            MySQLServer.session.execute(sql_script)
        MySQLServer.session.commit

    def __upsertData(self, fund_code=None, quote_time=None, sql_script=None, des_table=None, src_table=None, parameter=None):
        if (sql_script is not None) and (parameter is not None):
            MySQLServer.session.execute(sql_script, parameter)
        elif (sql_script is not None) and (parameter is None):
            MySQLServer.session.execute(sql_script)


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
        # Setting Table Name
        # des_table = self.getTable(des_table_name)
        '''
        insert_stat = insert(des_table).values(
            fund_code=bindparam('fund_code'),
            Q_1_inc_2018=bindparam('Q_1_inc_2018'),
            Q_2_inc_2018=bindparam('Q_2_inc_2018'),
            Q_3_inc_2018=bindparam('Q_3_inc_2018'),
            Q_4_inc_2018=bindparam('Q_4_inc_2018'),
            Y_inc_2018=bindparam('Y_inc_2018'),
            cls_avg_2018=bindparam('cls_avg_2018'),
            cls_rank_2018=bindparam('cls_rank_2018'),
            cls_tal_2018=bindparam('cls_tal_2018'),
            Q_1_inc_2017=bindparam('Q_1_inc_2017'),
            Q_2_inc_2017=bindparam('Q_2_inc_2017'),
            Q_3_inc_2017=bindparam('Q_3_inc_2017'),
            Q_4_inc_2017=bindparam('Q_4_inc_2017'),
            Y_inc_2017=bindparam('Y_inc_2017'),
              cls_avg_2017=bindparam('cls_avg_2017'),
              cls_rank_2017=bindparam('cls_rank_2017'),
              cls_tal_2017=bindparam('cls_tal_2017'),
              Q_1_inc_2016=bindparam('Q_1_inc_2016'),
              Q_2_inc_2016=bindparam('Q_2_inc_2016'),
              Q_3_inc_2016=bindparam('Q_3_inc_2016'),
              Q_4_inc_2016=bindparam('Q_4_inc_2016'),
              Y_inc_2016=bindparam('Y_inc_2016'),
              cls_avg_2016=bindparam('cls_avg_2016'),
              cls_rank_2016=bindparam('cls_rank_2016'),
              cls_tal_2016=bindparam('cls_tal_2016'),
              Q_1_inc_2015=bindparam('Q_1_inc_2015'),
              Q_2_inc_2015=bindparam('Q_2_inc_2015'),
              Q_3_inc_2015=bindparam('Q_3_inc_2015'),
              Q_4_inc_2015=bindparam('Q_4_inc_2015'),
              Y_inc_2015=bindparam('Y_inc_2015'),
              cls_avg_2015=bindparam('cls_avg_2015'),
              cls_rank_2015=bindparam('cls_rank_2015'),
              cls_tal_2015=bindparam('cls_tal_2015'),
              Q_1_inc_2014=bindparam('Q_1_inc_2014'),
              Q_2_inc_2014=bindparam('Q_2_inc_2014'),
              Q_3_inc_2014=bindparam('Q_3_inc_2014'),
              Q_4_inc_2014=bindparam('Q_4_inc_2014'),
              Y_inc_2014=bindparam('Y_inc_2014'),
              cls_avg_2014=bindparam('cls_avg_2014'),
              cls_rank_2014=bindparam('cls_rank_2014'),
              cls_tal_2014=bindparam('cls_tal_2014'),
              Q_1_inc_2013=bindparam('Q_1_inc_2013'),
              Q_2_inc_2013=bindparam('Q_2_inc_2013'),
              Q_3_inc_2013=bindparam('Q_3_inc_2013'),
              Q_4_inc_2013=bindparam('Q_4_inc_2013'),
              Y_inc_2013=bindparam('Y_inc_2013'),
              cls_avg_2013=bindparam('cls_avg_2013'),
              cls_rank_2013=bindparam('cls_rank_2013'),
              cls_tal_2013=bindparam('cls_tal_2013'),
              Q_1_inc_2012=bindparam('Q_1_inc_2012'),
              Q_2_inc_2012=bindparam('Q_2_inc_2012'),
              Q_3_inc_2012=bindparam('Q_3_inc_2012'),
              Q_4_inc_2012=bindparam('Q_4_inc_2012'),
              Y_inc_2012=bindparam('Y_inc_2012'),
              cls_avg_2012=bindparam('cls_avg_2012'),
              cls_rank_2012=bindparam('cls_rank_2012'),
              cls_tal_2012=bindparam('cls_tal_2012'),
              Q_1_inc_2011=bindparam('Q_1_inc_2011'),
              Q_2_inc_2011=bindparam('Q_2_inc_2011'),
              Q_3_inc_2011=bindparam('Q_3_inc_2011'),
              Q_4_inc_2011=bindparam('Q_4_inc_2011'),
              Y_inc_2011=bindparam('Y_inc_2011'),
              cls_avg_2011=bindparam('cls_avg_2011'),
              cls_rank_2011=bindparam('cls_rank_2011'),
              cls_tal_2011=bindparam('cls_tal_2011'))
        upsert_stat = insert_stat.on_duplicate_key_update(
            Q_1_inc_2018=insert_stat.inserted.Q_1_inc_2018,
            Q_2_inc_2018=insert_stat.inserted.Q_2_inc_2018,
            Q_3_inc_2018=insert_stat.inserted.Q_3_inc_2018,
            Q_4_inc_2018=insert_stat.inserted.Q_4_inc_2018,
            Y_inc_2018=insert_stat.inserted.Y_inc_2018,
            cls_avg_2018=insert_stat.inserted.cls_avg_2018,
            cls_rank_2018=insert_stat.inserted.cls_rank_2018,
            cls_tal_2018=insert_stat.inserted.cls_tal_2018,
            Q_1_inc_2017=insert_stat.inserted.Q_1_inc_2017,
            Q_2_inc_2017=insert_stat.inserted.Q_2_inc_2017,
            Q_3_inc_2017=insert_stat.inserted.Q_3_inc_2017, Q_4_inc_2017=insert_stat.inserted.Q_4_inc_2017,
            Y_inc_2017=insert_stat.inserted.Y_inc_2017, cls_avg_2017=insert_stat.inserted.cls_avg_2017,
            cls_rank_2017=insert_stat.inserted.cls_rank_2017, cls_tal_2017=insert_stat.inserted.cls_tal_2017,
            Q_1_inc_2016=insert_stat.inserted.Q_1_inc_2016, Q_2_inc_2016=insert_stat.inserted.Q_2_inc_2016,
            Q_3_inc_2016=insert_stat.inserted.Q_3_inc_2016, Q_4_inc_2016=insert_stat.inserted.Q_4_inc_2016,
            Y_inc_2016=insert_stat.inserted.Y_inc_2016, cls_avg_2016=insert_stat.inserted.cls_avg_2016,
            cls_rank_2016=insert_stat.inserted.cls_rank_2016, cls_tal_2016=insert_stat.inserted.cls_tal_2016,
            Q_1_inc_2015=insert_stat.inserted.Q_1_inc_2015, Q_2_inc_2015=insert_stat.inserted.Q_2_inc_2015,
            Q_3_inc_2015=insert_stat.inserted.Q_3_inc_2015, Q_4_inc_2015=insert_stat.inserted.Q_4_inc_2015,
            Y_inc_2015=insert_stat.inserted.Y_inc_2015, cls_avg_2015=insert_stat.inserted.cls_avg_2015,
            cls_rank_2015=insert_stat.inserted.cls_rank_2015, cls_tal_2015=insert_stat.inserted.cls_tal_2015,
            Q_1_inc_2014=insert_stat.inserted.Q_1_inc_2014, Q_2_inc_2014=insert_stat.inserted.Q_2_inc_2014,
            Q_3_inc_2014=insert_stat.inserted.Q_3_inc_2014, Q_4_inc_2014=insert_stat.inserted.Q_4_inc_2014,
            Y_inc_2014=insert_stat.inserted.Y_inc_2014, cls_avg_2014=insert_stat.inserted.cls_avg_2014,
            cls_rank_2014=insert_stat.inserted.cls_rank_2014, cls_tal_2014=insert_stat.inserted.cls_tal_2014,
            Q_1_inc_2013=insert_stat.inserted.Q_1_inc_2013, Q_2_inc_2013=insert_stat.inserted.Q_2_inc_2013,
            Q_3_inc_2013=insert_stat.inserted.Q_3_inc_2013, Q_4_inc_2013=insert_stat.inserted.Q_4_inc_2013,
            Y_inc_2013=insert_stat.inserted.Y_inc_2013, cls_avg_2013=insert_stat.inserted.cls_avg_2013,
            cls_rank_2013=insert_stat.inserted.cls_rank_2013, cls_tal_2013=insert_stat.inserted.cls_tal_2013,
            Q_1_inc_2012=insert_stat.inserted.Q_1_inc_2012, Q_2_inc_2012=insert_stat.inserted.Q_2_inc_2012,
            Q_3_inc_2012=insert_stat.inserted.Q_3_inc_2012, Q_4_inc_2012=insert_stat.inserted.Q_4_inc_2012,
            Y_inc_2012=insert_stat.inserted.Y_inc_2012, cls_avg_2012=insert_stat.inserted.cls_avg_2012,
            cls_rank_2012=insert_stat.inserted.cls_rank_2012, cls_tal_2012=insert_stat.inserted.cls_tal_2012,
            Q_1_inc_2011=insert_stat.inserted.Q_1_inc_2011, Q_2_inc_2011=insert_stat.inserted.Q_2_inc_2011,
            Q_3_inc_2011=insert_stat.inserted.Q_3_inc_2011, Q_4_inc_2011=insert_stat.inserted.Q_4_inc_2011,
            Y_inc_2011=insert_stat.inserted.Y_inc_2011, cls_avg_2011=insert_stat.inserted.cls_avg_2011,
            cls_rank_2011=insert_stat.inserted.cls_rank_2011, cls_tal_2011=insert_stat.inserted.cls_tal_2011)
        '''

        '''
        # Build insert_param's values
        pam_in = []
        pam_up = []

        for i in range(len(columns)):
            pam_in.append(columns[i])
            pam_in.append(bindparam)
            pam_in.append(columns[i])


        for i in range(len(columns)):
            insert_placeholder = ','.join(["{}"=bindparam("'{}'")] * len(columns))
            upsert_placeholder = ','.join(['{}={}.{}']*(len(columns)-1))

        val = insert_placeholder.format(*pam_in)
        print val
        insert_stat = insert(des_table).values(val)
        print insert_stat
        for i in range(1, len(columns)):
            pam_up.append(columns[i])
            #pam_up.append(insert_stat.inserted)
            pam_up.append(columns[i])
        '''
        parameters = {k: v for k, v in parameters[0].items() if v is not None}
        # print parameters

        cols = ','.join([key for key, value in parameters.items()])
        vals = '{}'.format(','.join([str(value) for key, value in parameters.items()]))

        up_vals = ','.join('{} = {}'.format(key, str(value)) for (key, value) in parameters.items())

        insert_stat = "insert into {} ({}) VALUES ({})".format(des_table_name, cols, vals)
        upsert_stat = "insert into {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(des_table_name,
                                                                                          cols, vals, up_vals)

        if func == 'upsert':

            return upsert_stat
        else:
            return insert_stat
