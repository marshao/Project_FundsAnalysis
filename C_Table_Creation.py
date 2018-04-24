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
'''


class tb_FundRankInClass(Base):
    __tablename__ = 'tb_FundRankInClass'

    fund_code = Column(String(10), ForeignKey(tb_FundList.c.fund_code), primary_key=True)
    quote_date = Column(DateTime(), primary_key=True)
    fund_rank_in_class = Column(Integer(), nullable=True)


Base.metadata.create_all(db_engine)
