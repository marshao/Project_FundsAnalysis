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
import re
from bs4 import BeautifulSoup
import time, datetime
import random
import pandas as pd
import C_MySQL_Server as db
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import bindparam

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

    def __getURL_NV(self, url, params, header, tries_num=5, sleep_time=0.1, time_out=10,max_retry = 5):
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
                res = requests.get(url, headers=header, params=params, timeout=time_out, proxies=self.proxy)
            else:
                res = requests.get(url, headers=header, params=params, timeout=time_out)
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
        tb_FundList = self.db_server.getTable('tb_FundList')

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
        parameters = []

        for idx in range(0, fund_count):
            datastring[idx] = datastring[idx].replace('"','')

            # long-status, 0=stop, 1=buy, 2=big amount long
            datastring[idx] = datastring[idx].replace("\xe5\xbc\x80\xe6\x94\xbe\xe7\x94\xb3\xe8\xb4\xad", "1")
            datastring[idx] = datastring[idx].replace("\xe6\x9a\x82\xe5\x81\x9c\xe7\x94\xb3\xe8\xb4\xad", "0")
            datastring[idx] = datastring[idx].replace("\xe9\x99\x90\xe5\xa4\xa7\xe9\xa2\x9d", "2")
            datastring[idx] = datastring[idx].replace("\xe5\x9c\xba\xe5\x86\x85\xe4\xba\xa4\xe6\x98\x93", "0")

            #short_status, 0=stop, 1=sale
            datastring[idx] = datastring[idx].replace("\xe5\xbc\x80\xe6\x94\xbe\xe8\xb5\x8e\xe5\x9b\x9e", "1")
            datastring[idx] = datastring[idx].replace("\xe6\x9a\x82\xe5\x81\x9c\xe8\xb5\x8e\xe5\x9b\x9e", "0")
            datastring[idx] = datastring[idx].replace("\xe5\xb0\x81\xe9\x97\xad\xe6\x9c\x9f", "0")

            datastring[idx] = datastring[idx].split(",")

            parameters.append({'fund_code': datastring[idx][0], 'fund_name': datastring[idx][1],
                               'long_status': datastring[idx][9], 'short_status': datastring[idx][10]})

            datastring[idx] = [datastring[idx][i] for i in (0,1,9,10)]

        # Tuncate table
        # self.db_server.processData(func='truncate', des_table='tb_FundList')

        # Use DF to save data
        # df_data = pd.DataFrame(datastring, columns=['fund_code', 'fund_name', 'long_status', 'short_status'])
        #df_data.to_sql('tb_FundList', if_exists='append', con=self.db_server.db_engine, index=False)

        insert_stat = insert(tb_FundList). \
            values(fund_code=bindparam('fund_code'),
                   fund_name=bindparam('fund_name'),
                   long_status=bindparam('long_status'),
                   short_status=bindparam('short_status'))

        upsert_stat = insert_stat.on_duplicate_key_update(
            long_status=insert_stat.inserted.long_status,
            short_status=insert_stat.inserted.short_status
        )

        # stat = [upsert_stat, upsert_stat]
        self.db_server.processData(func='upsert', sql_script=upsert_stat, parameter=parameters)

        return datastring

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

    def __getFundBaseInfor(self, fund_code=None, quote_time=None, func=None):
        '''
        获取基金概况基本信息
        :param fund_code:
        :param quote_time:
        :param func:
        :return:
        '''
        fund_url = 'http://fund.eastmoney.com/f10/jbgk_' + fund_code + '.html'
        result = {}
        res = self.__getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        # print soup
        try:
            result['fund_code'] = fund_code
            result['fund_name'] = soup.find_all(text=u"基金全称")[0].next_element.text.strip()
            result['fund_abbr_name'] = soup.find_all(text=u"基金简称")[0].next_element.text.strip()
            result['fund_type'] = soup.find_all(text=u"基金类型")[0].next_element.text.strip()
            result['issue_date'] = soup.find_all(text=u"发行日期")[0].next_element.text.strip()

            result['establish_date'] = soup.find_all(text=u"成立日期/规模")[0].next_element.text.split(u'/')[0].strip()
            result['establish_scale'] = soup.find_all(text=u"成立日期/规模")[0].next_element.text.split(u'/')[-1].strip()
            result['asset_value'] = soup.find_all(text=u"资产规模")[0].next_element.text.split(u'（')[0].strip()
            # Sometime the value are '---', that will stop the next step
            if soup.find_all(text=u"资产规模")[0].next_element.text.find('-') == -1:
                result['asset_value_date'] = \
                    soup.find_all(text=u"资产规模")[0].next_element.text.split(u'（')[1].split(u'）')[0].strip(u'截止至：')
            else:
                result['asset_value_date'] = None
            result['units'] = soup.find_all(text=u"份额规模")[0].next_element.text.strip().split(u'（')[0].strip()
            if soup.find_all(text=u"份额规模")[0].next_element.text == -1:
                result['units_date'] = soup.find_all(text=u"份额规模")[0].next_element.text.strip().split(u'（')[1].strip(
                    u'（截止至：）')
            else:
                result['units_date'] = None

            result['fund_company'] = soup.find_all(text=u"基金管理人")[0].next_element.text.strip()
            result['fund_trustee'] = soup.find_all(text=u"基金托管人")[0].next_element.text.strip()

            # Process Fund Manager Information
            result['fund_manager_name_1'] = soup.find_all(text=u"基金经理人")[0].next_element
            result['fund_manager_code_1'] = None
            result['fund_manager_code_2'] = None
            result['fund_manager_name_2'] = None
            result['fund_manager_code_3'] = None
            result['fund_manager_name_3'] = None

            result['total_div'] = soup.find_all(text=u"成立来分红")[0].next_element.text.strip()
            result['mgt_fee'] = soup.find_all(text=u"管理费率")[0].next_element.text.strip()
            result['trust_fee'] = soup.find_all(text=u"托管费率")[0].next_element.text.strip()
            result['sale_fee'] = soup.find_all(text=u"销售服务费率")[0].next_element.text.strip()
            result['buy_fee'] = soup.find_all(text=u"最高认购费率")[0].next_element.text.strip()
            result['buy_fee2'] = soup.find_all(text=u"最高申购费率")[0].next_element.text.strip()
            result['benchmark'] = soup.find_all(text=u"业绩比较基准")[0].next_element.text.strip(u'该基金暂未披露业绩比较基准')
            result['underlying'] = soup.find_all(text=u"跟踪标的")[0].next_element.text.strip(u'该基金无跟踪标的')
            result = self.__baseInforCleaning(result)
        except  Exception as e:
            print ''
            print 'Fund %s Data Cleaning error:' % result['fund_code']

        try:
            tb_FundInfo = self.db_server.getTable('tb_FundInfo')
            insert_stat = insert(tb_FundInfo). \
                values(fund_code=bindparam('fund_code'),
                       fund_name=bindparam('fund_name'),
                       fund_abbr_name=bindparam('fund_abbr_name'),
                       fund_type=bindparam('fund_type'),
                       issue_date=bindparam('issue_date'),
                       establish_date=bindparam('establish_date'),
                       establish_scale=bindparam('establish_scale'),
                       asset_value=bindparam('asset_value'),
                       asset_value_date=bindparam('asset_value_date'),
                       units=bindparam('units'),
                       units_date=bindparam('units_date'),
                       fund_manager_name_1=bindparam('fund_manager_name_1'),
                       fund_manager_name_2=bindparam('fund_manager_name_2'),
                       fund_manager_name_3=bindparam('fund_manager_name_3'),
                       fund_manager_code_1=bindparam('fund_manager_code_1'),
                       fund_manager_code_2=bindparam('fund_manager_code_2'),
                       fund_manager_code_3=bindparam('fund_manager_code_3'),
                       fund_trustee=bindparam('fund_trustee'),
                       fund_company=bindparam('fund_company'),
                       total_div=bindparam('total_div'),
                       mgt_fee=bindparam('mgt_fee'),
                       trust_fee=bindparam('trust_fee'),
                       sale_fee=bindparam('sale_fee'),
                       buy_fee=bindparam('buy_fee'),
                       buy_fee2=bindparam('buy_fee2'),
                       benchmark=bindparam('benchmark'),
                       underlying=bindparam('underlying')
                       )

            upsert_stat = insert_stat.on_duplicate_key_update(
                fund_name=insert_stat.inserted.fund_name,
                fund_abbr_name=insert_stat.inserted.fund_abbr_name,
                fund_type=insert_stat.inserted.fund_type,
                issue_date=insert_stat.inserted.issue_date,
                establish_date=insert_stat.inserted.establish_date,
                establish_scale=insert_stat.inserted.establish_scale,
                asset_value=insert_stat.inserted.asset_value,
                asset_value_date=insert_stat.inserted.asset_value_date,
                units=insert_stat.inserted.units,
                units_date=insert_stat.inserted.units_date,
                fund_manager_name_1=insert_stat.inserted.fund_manager_name_1,
                fund_manager_name_2=insert_stat.inserted.fund_manager_name_2,
                fund_manager_name_3=insert_stat.inserted.fund_manager_name_3,
                fund_manager_code_1=insert_stat.inserted.fund_manager_code_1,
                fund_manager_code_2=insert_stat.inserted.fund_manager_code_2,
                fund_manager_code_3=insert_stat.inserted.fund_manager_code_3,
                fund_trustee=insert_stat.inserted.fund_trustee,
                fund_company=insert_stat.inserted.fund_company,
                total_div=insert_stat.inserted.total_div,
                mgt_fee=insert_stat.inserted.mgt_fee,
                trust_fee=insert_stat.inserted.trust_fee,
                sale_fee=insert_stat.inserted.sale_fee,
                buy_fee=insert_stat.inserted.buy_fee,
                buy_fee2=insert_stat.inserted.buy_fee2,
                benchmark=insert_stat.inserted.benchmark,
                underlying=insert_stat.inserted.underlying
            )
            self.db_server.processData(func='upsert', sql_script=upsert_stat, parameter=result)
            # print (self.getCurrentTime(),'Fund Info Insert Sucess:', result['fund_code'],result['fund_name'],result['fund_abbr_name'],result['fund_manager'],result['funder'],result['establish_date'],result['establish_scale'],result['benchmark'] )
        except  Exception as e:
            print ''
            print 'Fund %s Data saving error:' % result['fund_code']

        return result

    def __baseInforCleaning(self, base_infor):
        try:
            # Elimite unneeded parentheses from data and retrun str only
            # print 'Parentheses'
            base_infor['mgt_fee'] = self.__eliminateParenthes(base_infor['mgt_fee'])
            base_infor['trust_fee'] = self.__eliminateParenthes(base_infor['trust_fee'])
            base_infor['buy_fee'] = self.__eliminateParenthes(base_infor['buy_fee'])
            base_infor['buy_fee2'] = self.__eliminateParenthes(base_infor['buy_fee2'])
            base_infor['sale_fee'] = self.__eliminateParenthes(base_infor['sale_fee'])
            base_infor['total_div'] = filter(str.isdigit, self.__eliminateParenthes(base_infor['total_div']))

            # print 'scale'
            # Change scales data into float
            base_infor['establish_scale'] = self.__scaleToFloat(base_infor['establish_scale'])
            base_infor['asset_value'] = self.__scaleToFloat(base_infor['asset_value'])
            base_infor['units'] = self.__scaleToFloat(base_infor['units'])

            # print 'date'
            # Change Chinese date str into datetime
            base_infor['units_date'] = self.__dateChtoEng(base_infor['units_date'])
            base_infor['asset_value_date'] = self.__dateChtoEng(base_infor['asset_value_date'])
            base_infor['issue_date'] = self.__dateChtoEng(base_infor['issue_date'])
            base_infor['establish_date'] = self.__dateChtoEng(base_infor['establish_date'])

            # print 'fee'
            # Change str percent fee into float
            base_infor['mgt_fee'] = self.__percentToFloat(base_infor['mgt_fee'])
            base_infor['trust_fee'] = self.__percentToFloat(base_infor['trust_fee'])
            base_infor['buy_fee'] = self.__percentToFloat(base_infor['buy_fee'])
            base_infor['buy_fee2'] = self.__percentToFloat(base_infor['buy_fee2'])
            base_infor['sale_fee'] = self.__percentToFloat(base_infor['sale_fee'])
            base_infor['total_div'] = self.__percentToFloat(base_infor['total_div'])

            # Process Fund_manager_name and codes:
            managers = base_infor['fund_manager_name_1'].find_all('a')
            for i in range(0, len(managers)):
                if i == 0:
                    base_infor['fund_manager_code_1'] = managers[i]['href'].strip(
                        'http://fund.eastmoney.com/manager/.html').encode('utf-8')
                    base_infor['fund_manager_name_1'] = managers[i].text.strip().encode('utf-8')
                if i == 1:
                    base_infor['fund_manager_code_2'] = managers[i]['href'].strip(
                        'http://fund.eastmoney.com/manager/.html').encode('utf-8')
                    base_infor['fund_manager_name_2'] = managers[i].text.strip().encode('utf-8')
                if i == 2:
                    base_infor['fund_manager_code_3'] = managers[i]['href'].strip(
                        'http://fund.eastmoney.com/manager/.html').encode('utf-8')
                    base_infor['fund_manager_name_3'] = managers[i].text.strip().encode('utf-8')

        except Exception as e:
            print 'Processing fund %s with error %s' % (base_infor['fund_code'], e)
        return base_infor

    def __eliminateParenthes(self, data):
        pos = data.find('%')
        data = data[0:pos].encode('utf-8')
        return data

    def __percentToFloat(self, data):
        if data.find('--') != -1:
            data = 0.0
        else:
            try:
                data = round((float(data) / 100), 4)
            except:
                data = None
        return data

    def __scaleToFloat(self, data):
        if data.find('-') != -1:
            data = 'null'
        else:
            data = float(data.encode('utf-8')[0:-6])
            return data

    def __dateChtoEng(self, data):
        if data is not None:
            datestr = filter(str.isdigit, data.encode('utf-8'))
            if len(datestr) == 8:
                y = int(datestr[0:4])
                m = int(datestr[4:6])
                d = int(datestr[6:8])
                return datetime.datetime(y, m, d)

    def __strToFloat(self, data):
        if data != '':
            float(data)
        else:
            data = None
        return data

    def __getFundManagerInfor(self, fund_code=None, quote_time=None, func=None):
        '''''
                获取基金经理数据。 基金投资分析关键在投资经理，后续在完善

                :param fund_code:
                :return:
                '''
        # Get instance of des table in db
        tb_FundManagerHistory = self.db_server.getTable('tb_FundManagerHistory')

        fund_url = 'http://fund.eastmoney.com/f10/jjjl_' + fund_code + '.html'
        res = self.__getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        parameters = []

        #Find Fund Manager basic Infor
        jl_intro = soup.findAll(attrs={'class':'jl_intro'})
        # Find Fund Manager Records
        jl_office = soup.findAll(attrs={'class': 'jl_office'})

        if len(jl_intro) != len(jl_office):
            print "JL intros numbers does not match JL office numbers in %s"%fund_code
            return
        for i in range(0, len(jl_intro)):
            a = jl_intro[i].findAll('a')
            a[1].encode('utf-8')
            manager_code = a[1]['href'].strip('http://fund.eastmoney.com/manager/.html').encode('utf-8')
            manager_name = a[1].text.encode('utf-8')

            jl_records = jl_office[i].findAll('tr')
            for tr in jl_records[1:(len(jl_records))]:
                td = tr.find_all('td')
                result = {}
                try:
                    result['fund_code'] = td[0].getText().strip().encode('utf-8')
                    result['fund_name'] = td[1].getText().strip().encode('utf-8')
                    result['fund_type'] = td[2].getText().strip().encode('utf-8')
                    result['manager_code'] = manager_code
                    result['manager_name'] = manager_name
                    result['start_date'] = td[3].getText().strip()
                    result['end_date'] = td[4].getText().strip()
                    result['period_days'] = td[5].getText().strip().encode('utf-8')
                    result['return_rate'] = td[6].getText().strip().encode('utf-8')
                    result['class_average_return'] = td[7].getText().strip().encode('utf-8')
                    result['class_return_rank'] = td[8].getText().strip().encode('utf-8')
                    result = self.__managerInforCleaning(result)
                    parameters.append(result)
                except  Exception as e:
                    print ('getFundManagers2 Data retriving', fund_code)
                    # print ('getFundManagers1', fund_code, fund_url, e)

        try:
            #mySQL.insertData('fund_managers_chg', result)
            #print parameters
            insert_stat = insert(tb_FundManagerHistory). \
                values(fund_code=bindparam('fund_code'),
                       fund_name=bindparam('fund_name'),
                       manager_code=bindparam('manager_code'),
                       manager_name=bindparam('manager_name'),
                       start_date=bindparam('start_date'),
                       end_date=bindparam('end_date'),
                       period_days=bindparam('period_days'),
                       return_rate=bindparam('return_rate'),
                       class_average_return=bindparam('class_average_return'),
                       class_return_rank=bindparam('class_return_rank'),
                       class_return_rank_percent=bindparam('class_return_rank_percent'),
                       class_funds_count=bindparam('class_funds_count')
                       )

            upsert_stat = insert_stat.on_duplicate_key_update(
                start_date=insert_stat.inserted.start_date,
                end_date=insert_stat.inserted.end_date,
                period_days=insert_stat.inserted.period_days,
                return_rate=insert_stat.inserted.return_rate,
                class_average_return=insert_stat.inserted.class_average_return,
                class_return_rank=insert_stat.inserted.class_return_rank,
                class_return_rank_percent=insert_stat.inserted.class_return_rank_percent,
                class_funds_count=insert_stat.inserted.class_funds_count
            )

            # stat = [upsert_stat, upsert_stat]
            self.db_server.processData(func='upsert', sql_script=upsert_stat, parameter=parameters)

        except  Exception as e:
            print ('getFundManagers2 DB Saving', fund_code)
            #print ('getFundManagers2', result['fund_code'], e)

    def __managerInforCleaning(self, result):
        try:
            #print result
            result['start_date'] = self.__dateChtoEng(result['start_date'])
            result['end_date'] = self.__dateChtoEng(result['end_date'])

            if result['return_rate'] != '':
                result['return_rate'] = self.__percentToFloat(self.__eliminateParenthes(result['return_rate']))
            else:
                result['return_rate'] = None
            if result['class_average_return'] != '':
                result['class_average_return'] = self.__percentToFloat(self.__eliminateParenthes(result['class_average_return']))
            else:
                result['class_average_return'] = None

            #Process class rerurn
            split = result['class_return_rank'].find('|')
            empty = result['class_return_rank'].find('-')
            if split == -1 or empty != -1:
                result['class_return_rank'] = None
                result['class_return_rank_percent'] = None
                result['class_funds_count'] = None
            else :
                rank = int(result['class_return_rank'][0:split])
                class_total = int(result['class_return_rank'][split + 1:])
                percent = round((rank * 1.0) / (class_total * 1.0), 2)
                result['class_return_rank'] = rank
                result['class_return_rank_percent'] = percent
                result['class_funds_count'] = class_total

            # Process period days
            y = result['period_days'].find('\xe5\xb9\xb4')
            if y != -1:
                year = int(result['period_days'][:y])
                day = int(result['period_days'][y + 6:-3])
                result['period_days'] = 365 * year + day
            elif result['period_days'].find('\xe5\xa4\xa9') != -1:
                year = 0
                day = int(result['period_days'][:-3])
                result['period_days'] = day
            else:
                result['period_days'] = None

        except Exception as e:
            print 'Manager history cleaning error with exception %s' % result['fund_code']
            #print 'Manager history cleaning error with exception %s'%e

        return result

    def __getFundNetValue(self, fund_code=None, quote_time=None, func=None):
        '''''
                获取基金净值数据，因为基金列表中是所有基金代码，一般净值型基金和货币基金数据稍有差异，下面根据数据表格长度判断是一般基金还是货币基金，分别入库
                :param fund_code:
                :return:
                '''
        # Get instance of des table in db
        tb_FundNetValue = self.db_server.getTable('tb_FundNetValue')

        pageSize = '1'
        fund_url = 'http://api.fund.eastmoney.com/f10/lsjz'
        header = {'Referer':'http://fund.eastmoney.com/f10/jjjz_%s.html'%fund_code,
                  'connnection':'keep-alive',
                  }

        parameters = (
            ('\n\ncallback','jQuery18307681534724135337_1523257055977'),
            ('fundCode',fund_code),
            ('pageIndex','1'),
            ('pageSize',pageSize),
            ('startDate',''),
            ('endDate',''),
            ('_','152332241\n\n6468'),
        )
        try:
            # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1
            '''''
            #寿险获取单个基金的第一页数据，里面返回的apidata 接口中包含了记录数、分页及数据文件等
            #这里暂按照字符串解析方式获取，既然是标准API接口，应该可以通过更高效的方式批量获取全部净值数据，待后续研究。这里传入基金代码、分页页码和每页的记录数。先简单查询一次获取总的记录数，再一次性获取所有历史净值
            首次初始化完成后，如果后续每天更新或者定期更新，只要修改下每页返回的记录参数即可
           '''
            #fund_url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=1&per=1'
            res = self.__getURL_NV(url=fund_url, header=header, params=parameters)
            # 获取历史净值的总记录数
            pageSize = res.text[(res.text.find('"TotalCount"')+13):(res.text.find('"Expansion"')-1)]
            parameters = (
                ('\n\ncallback', 'jQuery18307681534724135337_1523257055977'),
                ('fundCode', fund_code),
                ('pageIndex', '1'),
                ('pageSize', pageSize),
                ('startDate', ''),
                ('endDate', ''),
                ('_', '152332241\n\n6468'),
            )
        except  Exception as e:
            # print ( 'getFundNavRecordsCount', fund_code,  e)
            print ('getFundNavRecordsCount', fund_code)

        try:
            # 根据基金代码和总记录数，一次返回所有历史净值
            #fund_nav = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=1&per=' + records
            res = self.__getURL_NV(url=fund_url, header=header, params=parameters)
            records = res.text[(res.text.find('"FSRQ"')):(res.text.rfind('"FundType"')-3)].encode('utf-8')
            records = records.replace('"', '')
            records = records.split('},{')
        except  Exception as e:
            # print ('getFundNVFullList', fund_code, e)
            print ('getFundNVFullList', fund_code)

        sql_param = []
        try:
            for item in records:
                # build two dictionaries to break and convert data
                result = {}
                temp = {}
                for unit in item.split(','):
                    parts = unit.split(':')
                    temp[parts[0]] = parts[1]

                result['fund_code'] = fund_code
                result['quote_date'] = self.__dateChtoEng(temp['FSRQ'].replace('-', ''))
                result['unit_net_value'] = self.__strToFloat(temp['DWJZ'])
                result['cum_net_value'] = self.__strToFloat(temp['LJJZ'])
                result['div_record'] = temp['FHSP']

                if temp['JZZZL'] == '':
                    result['daily_chg_rate'] = None
                else:
                    result['daily_chg_rate'] = float(temp['JZZZL']) / 100

                sql_param.append(result)
                #print sql_param
        except  Exception as e:
            print ('getFundNavRecordsDetail', fund_code, e)
            # print ('getFundNavRecordsDetail', fund_code)

        try:
            insert_ignore_stat = insert(tb_FundNetValue).prefix_with('IGNORE'). \
                values(fund_code=bindparam('fund_code'),
                       quote_date=bindparam('quote_date'),
                       unit_net_value=bindparam('unit_net_value'),
                       cum_net_value=bindparam('cum_net_value'),
                       daily_chg_rate=bindparam('daily_chg_rate')
                       )

            insert_stat = insert(tb_FundNetValue). \
                values(fund_code=bindparam('fund_code'),
                       quote_date=bindparam('quote_date'),
                       unit_net_value=bindparam('unit_net_value'),
                       cum_net_value=bindparam('cum_net_value'),
                       daily_chg_rate=bindparam('daily_chg_rate')
                       )

            upsert_stat = insert_stat.on_duplicate_key_update(
                unit_net_value=insert_stat.inserted.unit_net_value,
                cum_net_value=insert_stat.inserted.cum_net_value,
                daily_chg_rate=insert_stat.inserted.daily_chg_rate
            )

            # stat = [upsert_stat, upsert_stat]

            self.db_server.processData(func='upsert', sql_script=upsert_stat, parameter=sql_param)

        except  Exception as e:
            print ('getFundNavSaving Error', fund_code, sql_param, e)
            #print ('getFundNavSaving Error', fund_code)
                # 如果是货币基金，获取万份收益和7日年化利率

        return

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
        #self.__getFundManagerInfor('570006')
        #self.__getFundManagerInfor('070018')
        # self.__getFundNetValue('003563')
        #self.__getFundBaseInfor('005488')
        #'''
        fund_list = self.__getFundCodes()
        count = len(fund_list)
        for i in range(len(fund_list)):
            fund_code = fund_list[i][0]
            self.__getFundBaseInfor(fund_code)
            self.__getFundNetValue(fund_code)
            self.__getFundManagerInfor(fund_code)
            if i % 10 == 0:
                print ('{}/{}').format(i, count)
            #'''



def main():
    fspider = FundSpider()
    fspider.getFundInforFromWeb()

if __name__ == "__main__":
    main()