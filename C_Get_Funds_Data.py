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
import os
import pandas as pd
import re
import C_MySQL_Server as db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, MetaData, exists, update, and_, select, bindparam

class FundSpider():
    def __init__(self):
        self.db_server = db.MySQLServer()
        self.db_engine = self.db_server.getEngine()
        self.header = self.__getHeader()
        self.session = self.db_server.getSession()
        self.isproxy = 0
        self.proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}

    def __getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    def __getURL(self, url, tries_num=5, sleep_time=0.1, time_out=10,max_retry = 5):
        '''''
            get函数，主要是为了实现网络中断后自动重连，同时为了兼容各种网站不同的反爬策略及，通过sleep时间和timeout动态调整来测试合适的网络连接参数；
            通过isproxy 来控制是否使用代理，以支持一些在内网办公的同学
            :param url:
            :param tries_num:  重试次数
            :param sleep_time: 休眠时间
            :param time_out: 连接超时参数
            :param max_retry: 最大重试次数，仅仅是为了递归使用
            :return: response
            '''

        sleep_time_p = sleep_time
        time_out_p = time_out
        tries_num_p = tries_num
        try:
            res = requests.Session()
            if self.isproxy == 1:
                res = requests.get(url, headers=self.header, timeout=time_out, proxies=self.proxy)
            else:
                res = requests.get(url, headers=self.header, timeout=time_out)
            res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
        except requests.RequestException as e:
            sleep_time_p = sleep_time_p + 10
            time_out_p = time_out_p + 10
            tries_num_p = tries_num_p - 1
            # 设置重试次数，最大timeout 时间和 最长休眠时间
            if tries_num_p > 0:
                time.sleep(sleep_time_p)
                print (
                self.__getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p, u'次 Retry Connection', e)
                return self.__getURL(url, tries_num_p, sleep_time_p, time_out_p, max_retry)
        return res

    def __getHeader(self):
        '''''
            随机生成User-Agent
            :return:
            '''
        head_connection = ['Keep-Alive', 'close']
        head_accept = ['text/html, application/xhtml+xml, */*']
        head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
        head_user_agent = ['Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                           'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                           'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                           'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                           'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                           'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                           'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                           'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11',
                           'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                           'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0'
                           ]
        result = {
            'Connection': head_connection[0],
            'Accept': head_accept[0],
            'Accept-Language': head_accept_language[1],
            'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
        }
        return result

    def __getFundCodes(self):
        '''
        This function get all open-end funds code, name, long_status, and short_status
        :return:
        '''
        #Get instance of des table in db
        # tb_FundList = self.db_server.getTable('DB_FundsAnalysis.tb_FundList')

        #Get Funds Name URL
        fund_url = 'http://fund.eastmoney.com/allfund.html'
        fund_url_2 = 'http://fund.eastmoney.com/jzzzl.html#os_0;isall_1;ft_|;pt_1'
        fund_url_3 = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,' \
                     'desc&page=1,9999&feature=|&dt=1521532639782&atfc=&onlySale=0'

        #Get URL Contents
        res = self.__getURL(fund_url_3)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Process URL Contents
        fullstring = soup.string.encode(encoding="UTF_8")
        beg_pos = fullstring.find('datas:[') + 8
        end_pos = fullstring.find('count')-3
        datastring = fullstring[beg_pos:end_pos].split("],[")
        fund_count = len(datastring)
        parameters = {}

        for idx in range(0, fund_count):
            datastring[idx] = datastring[idx].replace('"','')

            # long-status, 0=stop, 1=buy, 2=big amount long
            datastring[idx] = datastring[idx].replace("\xe5\xbc\x80\xe6\x94\xbe\xe7\x94\xb3\xe8\xb4\xad", "1")
            datastring[idx] = datastring[idx].replace("\xe6\x9a\x82\xe5\x81\x9c\xe7\x94\xb3\xe8\xb4\xad", "0")
            datastring[idx] = datastring[idx].replace("\xe9\x99\x90\xe5\xa4\xa7\xe9\xa2\x9d", "2")

            #short_status, 0=stop, 1=sale
            datastring[idx] = datastring[idx].replace("\xe5\xbc\x80\xe6\x94\xbe\xe8\xb5\x8e\xe5\x9b\x9e", "1")
            datastring[idx] = datastring[idx].replace("\xe6\x9a\x82\xe5\x81\x9c\xe8\xb5\x8e\xe5\x9b\x9e", "0")
            datastring[idx] = datastring[idx].replace("\xe5\xb0\x81\xe9\x97\xad\xe6\x9c\x9f", "0")

            datastring[idx] = datastring[idx].split(",")
            # parameters.append({'fund_code': datastring[idx][0], 'fund_name': datastring[idx][1],
            #                   'long_status':datastring[idx][9], 'short_status':datastring[idx][10]})
            datastring[idx] = [datastring[idx][i] for i in (0,1,9,10)]
        df_data = pd.DataFrame(datastring, columns=['fund_code', 'fund_name', 'long_status', 'short_status'])

        engine = create_engine(
            'mysql+mysqldb://marshao:123@10.175.10.231/DB_FundsAnalysis',
            encoding='utf-8', pool_size=150, echo=True)

        df_data.to_sql('tb_FundList', if_exists='replace', con=engine, index=False)
        #Upsert fund list into db table
        # self.db_server.processData(func='upsert', des_table=tb_FundList, parameter=datastring)
        '''
        update_stat = tb_FundList.update(). \
            values( fund_name=bindparam('fund_name'), \
                   long_status=bindparam('long_status'), short_status=bindparam('short_status')).\
            where (tb_FundList.c.fund_code == bindparam('fund_code'))

        upsert_stat = tb_FundList.insert(). \
            values(fund_code=bindparam('fund_code'), fund_name=bindparam('fund_name'), \
                   long_status=bindparam('long_status'), short_status=bindparam('short_status')). \
            where(~exists(update_stat))
        '''

    def __getFundCodes_alt(self):
        '''
        This function get all open-end funds code, name, long_status, and short_status
        :return:
        '''
        fund_url = 'http://fund.eastmoney.com/allfund.html'
        fund_url_2 = 'http://fund.eastmoney.com/jzzzl.html#os_0;isall_1;ft_|;pt_1'
        fund_url_3 = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,' \
                     'desc&page=1,9999&feature=|&dt=1521532639782&atfc=&onlySale=0'
        res = self.__getURL(fund_url_2)
        #soup = BeautifulSoup(res.text.decode('gbk', 'utf8'), 'html.parser')
        soup = BeautifulSoup(res.text, 'html.parser')
        result = pd.DataFrame()
        temp = {}
        #print soup.find_all(id="tableDiv", attrs={'class':'odd'})


        temp['fund_code'] = soup.find_all(attrs={'class':'bzdm'})
        temp['fund_name'] = soup.find_all(attrs={'class': 'tol'})
        temp['long_status']   = soup.find_all(attrs={'class':'sgzt'})
        temp['short_status']  = soup.find_all(attrs={'class':'shzt'})


        for key, value in temp.iteritems():
            l = []
            if key == 'fund_name':
                for each_v in value:
                    title = each_v.find('a')
                    l.append(title.get('title'))
            elif key=='long_status':
                for each_v in value:
                    each_v = each_v.string
                    if each_v == u'\xd4\xdd\xcd\xa3':
                        l.append(0)
                    elif each_v == u'\xcf\xde\xb4\xf3\xb6\xee':
                        l.append(2)
                    else:
                        l.append(1)
            elif key == 'short_status':
                for each_v in value:
                    each_v = each_v.string
                    if each_v == (u'\xbf\xaa\xb7\xc5'):
                        l.append(1)
                    else:
                        l.append(0)
            else:
                for each_v in value:
                    l.append(each_v.string)

            result[key] = l

        print result



    def __get_text(self, result):
        temp = []
        for each_code in result['fund_code']:
            temp.append(each_code.get_text())

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

    def getFundInforFromWeb(self, fund_code=None, func=None, quote_time=None, infor=None):
        self.__getFundCodes()

def main():
    fspider = FundSpider()
    fspider.getFundInforFromWeb()

if __name__ == "__main__":
    main()