# -*- coding: utf-8 -*-
# @Time    : 2019/08/05
# @Author  : coolchen

import datetime
import json
import logging
import os
import platform
import random
import re
import smtplib
import time
from email.mime.text import MIMEText
import pymysql
import redis
import requests
from fake_useragent import UserAgent
import chardet
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import sys,io

#告警邮件功能
msg_from = ''  # 发送方邮箱
passwd = ''  # 填入发送方邮箱的授权码
msg_to = ''  # 收件人邮箱
def smt(content):
    subject = "高德根据更换key值失败"  # 主题
    content = content  # 正文
    msg = MIMEText(content)
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to
    try:
        s = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 邮件服务器及端口号
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("发送成功")
    except:
        pass

# logging的设置
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

sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
user_name = "root"
user_password = "Biadmin@123"
database_ip = "10.0.1.73:3306"
database_name = "amazon_bdata"
database_all = "mysql+pymysql://" + user_name + ":" + user_password + "@" + database_ip + \
"/" + database_name + "?charset=utf8mb4"
engine = create_engine(database_all)

# engine=sqlalchemy.create_engine('mysql+mysqldb://{user}:{password}@{host}:3306/{database}'.format
#         (user='root',password='Biadmin@123',host='10.0.1.73',database='amazon_bdata'))

def encod(file):
    f = open(file, 'rb')
    f_charInfo = chardet.detect(f.read())
    encode = f_charInfo['encoding']
    return encode

# redis的队列设置
dbnum = 15
# redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=dbnum, encoding='utf-8', decode_responses=True)
# redis_conn = redis.Redis(host='47.115.181.162', port=6379, password='123456', db=dbnum, encoding='utf-8',decode_responses=True)
redis_conn = redis.Redis(host='10.0.1.201', port=6379, password='iA7gahY7l', db=dbnum, encoding='utf-8',decode_responses=True)


redis_name = 'irobotbox'
redis_all_city = '{}:all_city'.format(redis_name)
redis_task = '{}:task'.format(redis_name)
redis_downloadurl = '{}:downloadurl'.format(redis_name)
redis_downloadurl_backup = '{}:downloadurl_backup'.format(redis_name)
redis_task_backup = '{}:backup_task'.format(redis_name)
redis_error = '{}:error'.format(redis_name)
redis_result = '{}:result'.format(redis_name)
redis_failure_point = '{}:failure_point'.format(redis_name)

#来源表
# conn2 = pymysql.connect(host='rm-wz9bd2h2i9846lv92to.mysql.rds.aliyuncs.com',
#                              port=3306,
#                              user='spider_user',
#                              password='K845CTp8',
#                              db='apidb',
#                              charset='utf8')

# 结果表
result_conn = pymysql.connect(host='10.0.1.73',
                             port=3306,
                             user='root',
                             password='Biadmin@123',
                             db='amazon_bdata',
                             charset='utf8')
conn = result_conn

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


# 请求头的更换
def get_ua():
    while True:
        ua = UserAgent(use_cache_server=True)
        useragent = ua.random
        if len(useragent) > 90:
            return useragent
        else:
            continue

# 时间显示设置
def show_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

# 生成sql语句插入数据库
def create_sql(column_item, table_name):
    column = ' ('
    values = ' ('
    for i in column_item:
        if str(column_item[i]) == 'None':
            continue
        column += i + ","
        one_word = re.sub("'", "\"", str(column_item[i]).replace('\n', '').replace('\r', '').replace(
            '\t', '').replace('\\', '').replace('<em>',''))
        values += repr(one_word) + ","
    column = column[:-1] + ',insert_time'

    values = values[:-1] + ',CURRENT_TIMESTAMP'
    sql = 'insert into ' + table_name + column.lower() + ') values' + values + ');'
    return sql

# sql的链接

def select_db(sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        wait_crawler = cur.fetchall()  # fetchall    fetchamany   fetchone
        return wait_crawler

    except Exception as e:
        conn.rollback()
        print('出错信息：' + str(e))

def db_insert(sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print('error_info:{}'.format(e))

# ip的设置
class Proxies_url:
    '''
    proxies check service
    '''
    url = 'http://http.tiqu.qingjuhe.cn/getip?num=1&type=2&pack=20735&port=1&lb=1&pb=4&regions='
    # url = 'http://webapi.http.zhimacangku.com/getip?num=1&type=2&pro=&city=0&yys=0&port=1&pack=57439&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='

def get_proxy():
    """请求代理"""
    while True:
        try:
            r = requests.get(Proxies_url.url)
        except:
            continue
        try:
            json_str = json.loads(r.text)
            print(json_str)
        except:
            continue
        if json_str["msg"] == "请1秒后再试" or json_str["msg"] == "请2秒后再试":
            time.sleep(1)
            continue

        ip_port = str(json_str['data'][0]['ip']) + ':' + str(json_str['data'][0]['port'])
        print(ip_port)
        return ip_port