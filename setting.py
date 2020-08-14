# -*- coding: utf-8 -*-
# @Time    : 2019/08/05
# @Author  : coolchen

import datetime
import json
import platform
import random
import re
import smtplib
import time
from email.mime.text import MIMEText
import psycopg2
import pymysql
import redis
import requests
from fake_useragent import UserAgent

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

# redis与pg库的链接设置
dbnum = 15
#本地测试环境
# if 'Win' in platform.system():
#     redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=dbnum, encoding='utf-8', decode_responses=True)
#     conn = pymysql.connect(host='218.17.184.119', user='spider', password='aJX4$vQOgdrUJ$u0', port=33306, db='spider_db')
# #生产环境
# else:
#     redis_conn = redis.Redis(host='10.101.0.239', port=6379, password='abc123', db=dbnum, encoding='utf-8',
#                                   decode_responses=True)
#     conn = pymysql.connect(host='218.17.184.119', user='spider', password='aJX4$vQOgdrUJ$u0', port=33306, db='spider_db')


## redis的队列设置
today = 'cityloc'
redis_key = '20190425:key'
redis_all_city = '{}:allcity'.format(today)
redis_task = '{}:task'.format(today)
redis_total = '{}:total'.format(today)
redis_task_backup = '{}:backup_task'.format(today)
redis_error = '{}:error'.format(today)
redis_result = '{}:result'.format(today)
redis_failure_point = '{}:failure_point'.format(today)

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