# -*- coding:utf-8 -*-
# !/usr/local/bin/python


import requests
import C_MySQL_Server as db
import time
import datetime
import random
import pandas as pd


class StockIndexesSpider():
    def __init__(self):
        self.db_server = db.MySQLServer()
        self.db_engine = self.db_server.getEngine()
        self.header = self.__getHeader()
        self.session = self.db_server.getSession()
        self.isproxy = 0
        self.proxy = {"http": "http://10.179.10.14:808", "https": "http://10.179.10.14:808"}

    def __getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    def __getURL(self, url, tries_num=5, sleep_time=0.1, time_out=10, max_retry=5, proxy=0):
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
            if proxy == 1:
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
                    self.__getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p,
                    u'次 Retry Connection', e)
                return self.__getURL(url, tries_num_p, sleep_time_p, time_out_p, max_retry)
        return res

    def __getURL_NV(self, url, params, header, tries_num=5, sleep_time=0.1, time_out=10, max_retry=5, proxy=0):
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
            if proxy == 1:
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
                    self.__getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p,
                    u'次 Retry Connection', e)
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

    def __getRequestSession(self, proxy=0):

        res = requests.Session()
        if proxy == 1:
            res.proxies = self.proxy
            return res
        else:
            return res

    '''
    def __getIndicesFromDataReader(self, fund_code=None, period=None, model=None):
        ticker = ['^GSPC', 'SSE.000001']
        start = '2010-01-01'
        end = '2016-12-31'
        s = self.__getRequestSession(proxy=1)
        df = data.DataReader('DJI', data_source='morningstar', session=s)
        print df
        return
    '''
    def __getLocalIndices(self, amount=3000):

        ticker_sets = ['000001', '000016', '000300', '399001', '399006', '000903', '000905', '000012', '000013']
        error_funds = []
        for ticker in ticker_sets:
            fund_url = self.__idx_QQ_url(ticker, amount)
            try:
                res = self.__getURL(url=fund_url, proxy=0)
                data = res.text
                # print data
                result, error_funds = self.__process_idx_QQ_data(data, ticker, error_funds)
                sql_param, error_funds = self.__build_sql_param(ticker, result, error_funds)
                self.__save_idx_data(ticker, sql_param, error_funds)
            except  Exception as e:
                # print ('getFundNVFullList', fund_code, e)
                print ('getWebContents', ticker, e)
        return

    def __getForeignIndices(self, begin=1199116800):
        ticker_sets = [['GSPC', 'DJI', 'IXIC', 'NYA', 'XAX', 'RUT', 'VIX', 'FTSE',
                        'GDAXI', 'FCHI', 'N225', 'KS11', 'AXJO', 'TWII', 'GSPTSE', 'IPSA'],
                       ['MICEXINDEXCF.ME']]
        # dtime = datetime.datetime.strptime('2018-05-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        dtime = datetime.datetime.now()
        cdate = time.mktime(dtime.timetuple())
        # begin = -631008000 #(1950-01-01)
        # begin = 1514736000 #(2018-01-01)
        # begin = 1525104000 #(2018-05-01)
        # end = 1525708800  # (2018-05-08)
        end = int(cdate)
        #begin = 1199116800  # (2008-01-01)
        interval = '1d'
        error_funds = []
        for i in range(2):
            ticker_set = ticker_sets[i]
            for ticker in ticker_set:
                fund_url = self.__idx_yahoo_url(ticker, begin, end, interval, i)
                try:
                    res = self.__getURL(url=fund_url, proxy=0)
                    data = res.text
                    result, error_funds = self.__process_idx_yahoo_data(data, ticker, error_funds)
                    sql_param, error_funds = self.__build_sql_param(ticker, result, error_funds)
                    self.__save_idx_data(ticker, sql_param, error_funds)
                except  Exception as e:
                    # print ('getFundNVFullList', fund_code, e)
                    print ('getWebContents', ticker, e)
        return

    def __idx_yahoo_url(self, ticker, begin, end, interval, i):
        fund_url_0 = 'https://query1.finance.yahoo.com/v8/finance/chart/%5E{}?formatted=true' \
                     '&period1={}&period2={}&interval={}&events=div%7Csplit'.format(ticker, begin, end, interval)

        fund_url_1 = 'https://query1.finance.yahoo.com/v8/finance/chart/{}?formatted=true' \
                     '&period1={}&period2={}&interval={}&events=div%7Csplit'.format(ticker, begin, end, interval)
        fund_url_sets = [fund_url_0, fund_url_1]
        return fund_url_sets[i]

    def __idx_QQ_url(self, ticker, amount):
        if ticker[0] == '0':
            ticker = 'sh' + ticker
        else:
            ticker = 'sz' + ticker

        fund_url = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?var=kline_dayqfq&param={},' \
                   'day,,,{},qfq'.format(ticker, amount)

        return fund_url

    def __process_idx_yahoo_data(self, data, ticker, error_funds):
        try:
            result = {}
            #print data
            result['idx_name'] = ticker
            result['time_stamp'] = data[data.find('"timestamp"') + 13: data.find('"indicators"') - 2].split(',')
            result['time_stamp'] = [datetime.datetime.fromtimestamp(float(val)).strftime('%Y-%m-%d') for val in
                                    result['time_stamp']]
            rows = data[data.find('"indicators"') + 24: data.find('"error"') - 6].encode('utf-8')
            rows = rows.replace('"', '').replace('[{adjclose:', '').replace('}]', '').replace('[', '')
            rows = rows.split(']')
            for row in rows[:-1]:
                key = row[:row.find(':')].replace(',', '')
                value = row[row.find(':') + 1:].split(',')
                result[key] = value
                #print result
        except  Exception as e:
            print ('parser indice web data', ticker, e)
            error_funds.append(['parser indice web data', ticker])

        return result, error_funds

    def __process_idx_QQ_data(self, data, ticker, error_funds):
        try:
            result = {}
            records = data[(data.find("day") + 8): (data.find("qt") - 5)].split('"],["')
            time_stamp, open, close, high, low, volume = [], [], [], [], [], []

            result['idx_name'] = ticker
            for record in records:
                units = record.split('","')
                time_stamp.append(datetime.datetime.strptime(units[0], '%Y-%m-%d'))
                open.append(float(units[1]))
                close.append(float(units[2]))
                high.append(float(units[3]))
                low.append(float(units[4]))
                volume.append(float(units[5]))

            result['time_stamp'] = time_stamp
            result['open'] = open
            result['close'] = close
            result['adjclose'] = close
            result['high'] = high
            result['low'] = low
            result['volume'] = volume
        except  Exception as e:
            print ('parser indice web data', ticker, e)
            error_funds.append(['parser indice web data', ticker])
        return result, error_funds

    def __build_sql_param(self, ticker, result, error_funds):
        try:
            sql_param = []
            #print result
            for i in range(len(result['time_stamp'])):
                param = {}
                param['idx_name'] = '"{}"'.format(ticker)
                param['quote_date'] = '"{}"'.format(result['time_stamp'][i])
                param['open'] = result['open'][i]
                param['close'] = result['close'][i]
                param['adjclose'] = result['adjclose'][i]
                param['high'] = result['high'][i]
                param['low'] = result['low'][i]
                param['volume'] = result['volume'][i]
                sql_param.append(param)
        except  Exception as e:
            print ('build indice sql params', ticker, e)
            error_funds.append(['build indice sql params', ticker])
        return sql_param, error_funds

    def __save_idx_data(self, ticker, sql_param, error_funds):
        try:
            upsert_stat = self.db_server.buildQuery(func='upsert', parameters=sql_param,
                                                    des_table_name='tb_HistoryIndices')
            self.db_server.processData(func='upsert', sql_script=upsert_stat)
            print "{} is processed ".format(ticker)
        except  Exception as e:
            print ('data saving error ', e)
            error_funds.append(['saveIndicesContents', ticker])
            # self.__toPickles(error_funds, 'error_history_indices.ticker')
        return

    def saveIdxDataInCSV(self):
        try:
            sql_query = 'select a.idx_name from (' \
                        'SELECT idx_name ,count(adjclose) as cc FROM DB_FundsAnalysis.tb_HistoryIndices group by idx_name) a ' \
                        'where a.cc >= 300;'
            idx_names = self.session.execute(sql_query).fetchall()
        except Exception as e:
            print ("Index code error", e)

        try:
            df_indices = pd.DataFrame()
            for idx_name in idx_names:
                # print idx_name[0]
                sql_query = 'select adjclose, quote_date from DB_FundsAnalysis.tb_HistoryIndices where idx_name = "%s" order by quote_date DESC limit 1000' % (
                idx_name[0])
                df_temp = pd.read_sql(sql_query, con=self.db_engine)
                df_temp.rename(columns={'adjclose': idx_name[0]}, inplace=True)
                # print df_temp
                if df_indices.empty:
                    df_indices = df_temp
                else:
                    df_indices = df_indices.merge(right=df_temp, how='outer', on='quote_date')
            # df_indices.set_index('quote_date')
            # df_indices.to_csv('indices_data.csv', header=True, sep=',', float_format='%.4f', index=0)
            df_indices.to_csv('indices_data.csv', header=True, sep=',', index=0)
            # print df_indices
        except Exception as e:
            print e


    def getData(self):
        self.db_server.processData(func='truncate', des_table='tb_HistoryIndices')
        self.__getForeignIndices()
        self.__getLocalIndices()


def main():
    idx = StockIndexesSpider()
    idx.getData()
    idx.saveIdxDataInCSV()

if __name__ == "__main__":
    main()
