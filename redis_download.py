# -*- coding: utf-8 -*-
# @Time    : 2019/08/05
# @Author  : coolchen

import json
import time
import redis
# redis的设置
class SpiderConfig:
    RedisAddress = '127.0.0.1'
    RedisPort = '6379'

class RedisQueue(object):
    # redis中取放数据的函数设置
    """Simple Queue with Redis Backend"""
    def __init__(self, name, namespace, redis_conn):
        self.__db = redis_conn
        self.key = '%s:%s' % (namespace, name)

    def qsize(self):
        return self.__db.llen(self.key)

    def empty(self):
        return self.qsize() == 0

    def put(self, item):
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)
        if item:
            item = item[1]
        return item

    def get_nowait(self):
        return self.get(False)

class Put(object):
    def __init__(self):
        super(Put, self).__init__()
        self.redis_conn = redis.Redis(host=SpiderConfig.RedisAddress, port=SpiderConfig.RedisPort, db=15)
        self.z1 = RedisQueue(name='result', namespace='city', redis_conn=self.redis_conn)
        self.ff = open('result.txt', 'a+', encoding='utf-8')
        self.num = 0
    def put_token(self):
        while True:
            print(self.z1.qsize())
            item1 = str(self.z1.get(), encoding='utf-8')
            print(item1)
            self.ff.write(item1 + '\n')

if __name__ == '__main__':
    Put().put_token()