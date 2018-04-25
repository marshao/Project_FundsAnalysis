# -*- coding:utf-8 -*-
# !/usr/local/bin/python
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
from sqlalchemy import Table, Column, Integer, DECIMAL, DateTime, String, \
    MetaData, exists, update, and_, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()
db_engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis?charset=utf8',
                          encoding='utf-8', pool_size=150, echo=False)
DBSession = sessionmaker(bind=db_engine)
session = DBSession()
meta = MetaData(db_engine)
tb_FundList = Table('tb_FundList', meta, autoload=True)

'''
### Created Tables
class tb_FundCumIncomeRate_1M(Base):
    __tablename__ = 'tb_FundCumIncomeRate_1M'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))


class tb_FundCumIncomeRate_3M(Base):
    __tablename__ = 'tb_FundCumIncomeRate_3M'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))


class tb_FundCumIncomeRate_6M(Base):
    __tablename__ = 'tb_FundCumIncomeRate_6M'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))


class tb_FundCumIncomeRate_1Y(Base):
    __tablename__ = 'tb_FundCumIncomeRate_1Y'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))


class tb_FundCumIncomeRate_3Y(Base):
    __tablename__ = 'tb_FundCumIncomeRate_3Y'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))


class tb_FundCumIncomeRate_5Y(Base):
    __tablename__ = 'tb_FundCumIncomeRate_5Y'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))


class tb_FundCumIncomeRate_All(Base):
    __tablename__ = 'tb_FundCumIncomeRate_All'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    sh300idx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    shidx_cum_income_rate = Column(DECIMAL(8, 4), nullable=True)
    created_date = Column(DateTime(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))



class tb_FundRankInClass(Base):
    __tablename__ = 'tb_FundRankInClass'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_rank_in_class = Column(Integer(), nullable=True)



class tb_FundRankInPercent(Base):
    __tablename__ = 'tb_FundRankInPercent'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_rank_in_percent = Column(DECIMAL(8, 5), nullable=True)


class tb_FundPeriodicIncreaseDetail(Base):
    __tablename__ = 'tb_FundPeriodicIncreaseDetail'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    this_year_inc = Column(DECIMAL(6,4), nullable=True)
    this_year_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    this_year_sh300 = Column(DECIMAL(6, 4), nullable=True)
    this_year_cls_rank = Column(Integer, nullable=True)
    this_year_cls_tal = Column(Integer, nullable=True)
    this_year_rank_chg = Column(Integer, nullable=True)
    this_year_cls_mark = Column(String(10), nullable=True)
    last_week_inc = Column(DECIMAL(6, 4), nullable=True)
    last_week_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_week_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_week_cls_rank = Column(Integer, nullable=True)
    last_week_cls_tal = Column(Integer, nullable=True)
    last_week_rank_chg = Column(Integer, nullable=True)
    last_week_cls_mark = Column(String(10), nullable=True)
    last_month_inc = Column(DECIMAL(6, 4), nullable=True)
    last_month_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_month_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_month_cls_rank = Column(Integer, nullable=True)
    last_month_cls_tal = Column(Integer, nullable=True)
    last_month_rank_chg = Column(Integer, nullable=True)
    last_month_cls_mark = Column(String(10), nullable=True)
    last_3_month_inc = Column(DECIMAL(6, 4), nullable=True)
    last_3_month_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_3_month_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_3_month_cls_rank = Column(Integer, nullable=True)
    last_3_month_cls_tal = Column(Integer, nullable=True)
    last_3_month_rank_chg = Column(Integer, nullable=True)
    last_3_month_cls_mark = Column(String(10), nullable=True)
    last_6_month_inc = Column(DECIMAL(6, 4), nullable=True)
    last_6_month_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_6_month_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_6_month_cls_rank = Column(Integer, nullable=True)
    last_6_month_cls_tal = Column(Integer, nullable=True)
    last_6_month_rank_chg = Column(Integer, nullable=True)
    last_6_month_cls_mark = Column(String(10), nullable=True)
    last_year_inc = Column(DECIMAL(6, 4), nullable=True)
    last_year_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_year_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_year_cls_rank = Column(Integer, nullable=True)
    last_year_cls_tal = Column(Integer, nullable=True)
    last_year_rank_chg = Column(Integer, nullable=True)
    last_year_cls_mark = Column(String(10), nullable=True)
    last_2_year_inc = Column(DECIMAL(6, 4), nullable=True)
    last_2_year_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_2_year_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_2_year_cls_rank = Column(Integer, nullable=True)
    last_2_year_cls_tal = Column(Integer, nullable=True)
    last_2_year_rank_chg = Column(Integer, nullable=True)
    last_2_year_cls_mark = Column(String(10), nullable=True)
    last_3_year_inc = Column(DECIMAL(6, 4), nullable=True)
    last_3_year_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_3_year_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_3_year_cls_rank = Column(Integer, nullable=True)
    last_3_year_cls_tal = Column(Integer, nullable=True)
    last_3_year_rank_chg = Column(Integer, nullable=True)
    last_3_year_cls_mark = Column(String(10), nullable=True)
    last_5_year_inc = Column(DECIMAL(6, 4), nullable=True)
    last_5_year_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    last_5_year_sh300 = Column(DECIMAL(6, 4), nullable=True)
    last_5_year_cls_rank = Column(Integer, nullable=True)
    last_5_year_cls_tal = Column(Integer, nullable=True)
    last_5_year_rank_chg = Column(Integer, nullable=True)
    last_5_year_cls_mark = Column(String(10), nullable=True)
    ever_since_inc = Column(DECIMAL(6, 4), nullable=True)
    ever_since_cls_avg = Column(DECIMAL(6, 4), nullable=True)
    ever_since_sh300 = Column(DECIMAL(6, 4), nullable=True)
    ever_since_cls_rank = Column(Integer, nullable=True)
    ever_since_cls_tal = Column(Integer, nullable=True)
    ever_since_rank_chg = Column(Integer, nullable=True)
    ever_since_cls_mark = Column(String(10), nullable=True)


class tb_FundYearQuarterIncreaseDetail(Base):
    __tablename__ = 'tb_FundYearQuarterIncreaseDetail'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    Q_1_inc_2018 = Column(DECIMAL(6,4), nullable=True)
    Q_2_inc_2018 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2018 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2018 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2018 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2018 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2018 = Column(Integer, nullable=True)
    cls_tal_2018 = Column(Integer, nullable=True)
    Q_1_inc_2017 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2017 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2017 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2017 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2017 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2017 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2017 = Column(Integer, nullable=True)
    cls_tal_2017 = Column(Integer, nullable=True)
    Q_1_inc_2016 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2016 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2016 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2016 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2016 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2016 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2016 = Column(Integer, nullable=True)
    cls_tal_2016 = Column(Integer, nullable=True)
    Q_1_inc_2015 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2015 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2015 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2015 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2015 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2015 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2015 = Column(Integer, nullable=True)
    cls_tal_2015 = Column(Integer, nullable=True)
    Q_1_inc_2014 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2014 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2014 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2014 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2014 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2014 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2014 = Column(Integer, nullable=True)
    cls_tal_2014 = Column(Integer, nullable=True)
    Q_1_inc_2013 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2013 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2013 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2013 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2013 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2013 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2013 = Column(Integer, nullable=True)
    cls_tal_2013 = Column(Integer, nullable=True)
    Q_1_inc_2012 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2012 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2012 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2012 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2012 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2012 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2012 = Column(Integer, nullable=True)
    cls_tal_2012 = Column(Integer, nullable=True)
    Q_1_inc_2011 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2011 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2011 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2011 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2011 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2011 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2011 = Column(Integer, nullable=True)
    cls_tal_2011 = Column(Integer, nullable=True)
    Q_1_inc_2010 = Column(DECIMAL(6, 4), nullable=True)
    Q_2_inc_2010 = Column(DECIMAL(6, 4), nullable=True)
    Q_3_inc_2010 = Column(DECIMAL(6, 4), nullable=True)
    Q_4_inc_2010 = Column(DECIMAL(6, 4), nullable=True)
    Y_inc_2010 = Column(DECIMAL(6, 4), nullable=True)
    cls_avg_2010 = Column(DECIMAL(6, 4), nullable=True)
    cls_rank_2010 = Column(Integer, nullable=True)
    cls_tal_2010 = Column(Integer, nullable=True)
'''

Base.metadata.create_all(db_engine)
