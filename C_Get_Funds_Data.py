# -*- coding:utf-8 -*-
#!/usr/local/bin/python
############################################################################
'''''
# 程序：东方财富网基金数据爬取
# 功能：抓取东方财富网上基金相关数据
# 创建时间：2018/03/19 基金概况数据
# 更新历史：2017/02/15 增加基金净值数据
#
# 使用库：requests、BeautifulSoup4、pymysql,pandas
# 作者：Guan Hao
'''
#############################################################################
import requests
from bs4 import BeautifulSoup
import time
import random
import pymysql
import os
import pandas as pd
import re
import C_GlobalVariable as glb
import C_MySQL_Server as mysql

class FundSpider:
    def __init__(self):
        pass

    def getFundInforFromWeb(self, fund_code=None, func=None, quote_time=None, infor=None):
        pass

    def __getFundCodes(self):
        pass

    def __getURL(self):
        pass

    def __getCurrentTime(self):
        pass

    def __getHeader(self):
        pass

    def __getFundBaseInfor(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundManagerInfor(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundNetValue(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundDivident(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundPositionConfiguration(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundIndustryConfiguration(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getCapitalConfiguration(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundNetAsset(self, fund_code=None, quote_time=None, func=None):
        pass

    def __getFundFinancialIndicators(self, fund_code=None, quote_time=None, func=None):
        pass

def main():
    pass


if __name__ == "__main__":
    main()