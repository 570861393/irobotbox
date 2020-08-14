# -*- coding: utf-8 -*-
# @Time    : 2019/08/05
# @Author  : coolchen
import os
import sys
import threading
import time
import traceback
from urllib.parse import quote
import requests
import datetime
from setting import *
from lxml import etree
import re
import logging  # 引入logging模块
import os.path
import time
# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Log等级总开关
# 第二步，创建一个handler，用于写入日志文件
rq = time.strftime('%Y%m%d', time.localtime(time.time()))
log_path = os.path.dirname(os.getcwd()) + '/Logs/'
log_name = rq + '.log'
logfile = log_name
fh = logging.FileHandler(logfile, mode='w+')
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
# 第三步，定义handler的输出格式
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
# 第四步，将logger添加到handler里面
logger.addHandler(fh)

redis_conn = redis.Redis(host='47.115.181.162', port=6379, password='123456', db=dbnum, encoding='utf-8',
                                      decode_responses=True)


class Myspider(object):
    def __init__(self):
        self.num = 0
        self.thread_num = 1 #多线程的个数
        self.keynum = 0
        self.browser_list = {}
        self.headers={

        }
        # 多线程ip的设置
        # for i in range(self.thread_num):
        #     self.browser_list[i] = get_proxy()
        # self.ff = open('cityname.txt', 'r', encoding='utf-8')
        # self.pp = open('error.txt', 'a+', encoding='utf-8')
        self.headers={
            'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding':'gzip, deflate, br',
            # 'accept-language':'zh-CN,zh;q=0.9',
            'cache-control':'max-age=0',
            'sec-fetch-dest':'document',
            'sec-fetch-mode':'navigate',
            'sec-fetch-site':'none',
            'sec-fetch-user':'?1',
            'upgrade-insecure-requests':'1',
            'user-agent':UserAgent().random,
            'x-client-data':'CJW2yQEIprbJAQjEtskBCKmdygEImbXKAQj+vMoBCJm9ygEIi7/KAQjnyMoBCOnIygEI78nKAQi0y8oBGPC/ygE=',
        }

        dbnum = 15
        #测试环境
        # self.redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=dbnum, encoding='utf-8', decode_responses=True)


        #来源表
        self.conn2 = pymysql.connect(host='rm-wz9bd2h2i9846lv92to.mysql.rds.aliyuncs.com',
                                     port=3306,
                                     user='spider_user',
                                     password='K845CTp8',
                                     db='apidb',
                                     charset='utf8')

        # 结果表
        self.conn1 = pymysql.connect(host='rm-wz9bd2h2i9846lv92to.mysql.rds.aliyuncs.com',
                                     port=3306,
                                     user='spider_user',
                                     password='K845CTp8',
                                     db='datav',
                                     charset='utf8')

        # mysql--spider
        # self.conn1 = pymysql.connect(host='218.17.184.119',
        #                             port=33306,
        #                             user='spider',
        #                             password='aJX4$vQOgdrUJ$u0',
        #                             db='spider_db',
        #                             charset='utf8')
        # # mysql--aliyun
        # self.conn2 = pymysql.connect(host='rm-wz9bd2h2i9846lv92to.mysql.rds.aliyuncs.com',
        #                              port=3306,
        #                              user='spider_user',
        #                              password='K845CTp8',
        #                              db='datav',
        #                              charset='utf8')
        # # mysql--10.0.2.108
        # self.conn3 = pymysql.connect(host='10.0.2.108',
        #                              port=3306,
        #                              user='ops',
        #                              password='jS0rarzFQIltwJCC',
        #                              db='dev2_nextop',
        #                              charset='utf8')

    def create_sql(self,column_item, table_name):
        column = ' ('
        values = ' ('
        for i in column_item:
            if str(column_item[i]) == 'None':
                continue
            column += i + ","
            one_word = re.sub("'", "\"", str(column_item[i]).replace('\n', '').replace('\r', '').replace(
                '\t', '').replace('\\', '').replace('<em>', ''))
            values += repr(one_word) + ","
        column = column[:-1] + ',insert_time'
        values = values[:-1] + ',CURRENT_TIMESTAMP'
        sql = 'insert into ' + table_name + column.lower() + ') values' + values + ');'
        return sql

    # sql的链接
    # def select_db(self,sql):
    #     try:
    #         cur = self.conn.cursor()
    #         cur.execute(sql)
    #         wait_crawler = cur.fetchall()  # fetchall    fetchamany   fetchone
    #         return wait_crawler
    #     except Exception as e:
    #         self.conn.rollback()
    #         print('出错信息：' + str(e))

    def db_insert(self,sql):
        try:
            self.conn1.ping(reconnect=True)
            cur = self.conn1.cursor()
            cur.execute(sql)
            self.conn1.commit()
            print('数据插入成功')
        except Exception as e:
            print('mysql连接出错信息:{}'.format(e))
            self.conn1.rollback()

    # 清除数据中的脏乱字符
    def replaceall(self,word):
        return word.replace('\n','').replace(' ','').replace('\t','').replace('\xa0','').replace('\u3000','')

    # 放入任务到redis用于多线程
    def redis_put_task(self):
        for i in self.ff:
            item = i.strip('\n')
            redis_conn.rpush(redis_task,str(item))

    def mysql_put_task(self):
        today = datetime.date.today()
        ts = int(round(time.mktime(time.strptime(str(today), "%Y-%m-%d"))*1000))
        print(ts)
        cur = self.conn2.cursor()
        sql = 'SELECT city FROM test_amazon_shipping_address where create_time < {}'.format(ts)
        cur.execute(sql)
        result = cur.fetchall()
        for item in result:
            print(item[0])
            if redis_conn.sadd(redis_all_city,item[0]) == 1:
                redis_conn.rpush(redis_task,item[0])
    # 爬取核心代码
    def spider_start(self,thread_num):
        errornum=0
        s = 0
        while redis_conn.llen(redis_task) != 0:
            item2 = redis_conn.rpoplpush(redis_task,redis_task_backup)
            city=str(item2.strip('\n'))
            try:
                url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&key=AIzaSyBwJ3Z3aTdEu39gbpNcjWZ4OVhBVb5VlqA&language=us'.format(city)
                print(url)
                # proxies = {'https': "http://" + str(self.browser_list[thread_num]), }
                res = requests.get(url=url,headers=self.headers,timeout=10)
                item = res.json()
                print(item)
                if 'ZERO_RESULTS' in str(item) or 'REQUEST_DENIED' in str(item):
                    logger.error('搜索结果有误的城市:'+city)
                    print('搜索结果有误')
                    redis_conn.lrem(redis_task_backup, 0, item2)
                    continue
                dc = {}
                dc['cityname'] = city
                name = item['results'][0]['address_components'][0]
                country = item['results'][0]['address_components']
                for cool in country:
                    if cool['types'] == ['country', 'political']:
                        country=cool['long_name']
                        country_code = cool['short_name']

                print(country)
                dc['country'] = country
                dc['country_code'] = country_code
                dc['long_name'] = name['long_name']
                longname = dc['long_name'].replace(' ','')
                if len(longname)<3 or longname.isdigit() == True:
                    print('搜索结果不对')
                    logger.error('搜索结果不匹配的城市'+city)
                    redis_conn.lrem(redis_task_backup, 0, item2)
                    continue
                dc['short_name'] = name['short_name']
                dc['types'] = str(name['types'])
                formatted_address = item['results'][0]['formatted_address']
                dc['formatted_address'] = formatted_address
                loc = item['results'][0]['geometry']['location']
                location_type = item['results'][0]['geometry']['location_type']
                dc['location_type'] = location_type
                dc['lat'] = loc['lat']
                dc['lon'] = loc['lng']
                #导入到redis
                # redis_conn.rpush(redis_result, total)
                # redis_conn.lrem(redis_task_backup, 0, item2)
                # errornum = 0
                # print(item1)

                #直接导入mysql
                print(dc)
                insert_sql = self.create_sql(dc,'dim_city_location_test')
                print(insert_sql)
                self.db_insert(insert_sql)
                redis_conn.lrem(redis_task_backup, 0, item2)
            except:
                errornum+=1
                print('出错次数为'+str(errornum))
                traceback.print_exc()
                print('出现问题，任务放回')
                time.sleep(2)
                continue

    # 检查任务是否清空
    def whether_task(self):
        if redis_conn.llen(redis_task) == 0 and redis_conn.llen(redis_task_backup) == 0:
            print('任务队列已经清空')
            time.sleep(1)
            os._exit(0)
        else:
            pass

    # 守护线程，保证任务的不丢失
    def checkthread(self, initThreadsName):
        while True:
            self.whether_task()
            if redis_conn.llen(redis_task) == 0:
                sys.exit()
            newThreadsName = []
            for i in threading.enumerate():
                # TODO 记录正在运行的线程
                newThreadsName.append(i.getName())

            # print(newThreadsName)
            # TODO 判断有没有线程中途挂掉 如果有就重启线程
            for oldname in initThreadsName:
                if oldname in newThreadsName:
                    pass
                else:
                    print(oldname)
                    thread = threading.Thread(target=self.spider_start,args=(oldname,))
                    thread.setName(oldname)
                    thread.start()
                    print('重新启动了 线程：{}'.format(oldname))
            time.sleep(30)

    # 多线程的设置
    def thread_start(self):
        global recursion_num
        thread_list = []
        init_thread_name = []  # TODO 记录线程名
        for i in range(self.thread_num):
            thread = threading.Thread(target=self.spider_start,args=(i,))
            # TODO 给线程赋值
            thread.setName(i)
            thread_list.append(thread)

        for thread in thread_list:
            thread.start()
            time.sleep(0.1)
        # TODO 获取初始化的线程对象
        init = threading.enumerate()
        for i in init:
            # TODO 保存初始化线程名字
            init_thread_name.append(i.getName())
        time.sleep(1)
        thread_pro = threading.Thread(target=self.checkthread, args=(init_thread_name,))
        thread_pro.start()
        for thread in thread_list:
            thread.join()
        time.sleep(10)
        while redis_conn.llen(redis_task_backup) != 0:  # 多线程导致同时获取但是只有一个被测试
            redis_conn.rpoplpush(redis_task_backup, redis_task)
        recursion_num += 1
        if recursion_num > 10 and redis_conn.llen(redis_task) <= 10:
            redis_conn.delete(redis_task_backup)
            print('删除redis_task_backup')
            os._exit(0)
        time.sleep(1)
        self.thread_start()
        thread_pro.join()

recursion_num = 0

if __name__ == '__main__':
    Myspider = Myspider()
    Myspider.mysql_put_task()  #放入任务的程序
    # Myspider.redis_put_task()
    Myspider.thread_start() #程序启动