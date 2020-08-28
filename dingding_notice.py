import base64
import hashlib
import hmac
import json
import logging
import urllib
import time
import requests

# https://oapi.dingtalk.com/robot/send?access_token=854a52ffd8a8ee3923eb6f74bec7e37967465062b3d1d625ba7b2bac7bed1a1f

def __dingtalk_robot_signature(secret):
    # 当前时间戳
    timestamp = int(round(time.time() * 1000))
    # 密钥
    secret_enc = secret.encode('utf-8')

    # 加签
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    # 签名加密
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return timestamp, sign


def dingtalk_robot(content, atMobiles, isAtAll):
    """
    钉钉群机器人
    :param content: 要发送的内容
    :param atMobiles: 指定艾特的人
    :param isAtAll: 是否艾特全部？
    :return:
    """
    secret = 'SEC419a1044558c00e590134c2c6252f276735affb74c813be4b0f0e9c194df5fee'
    token ='854a52ffd8a8ee3923eb6f74bec7e37967465062b3d1d625ba7b2bac7bed1a1f'
    # 获取钉钉群机器人🤖签名.

    signature = __dingtalk_robot_signature(secret)
    timestamp = signature[0]
    sign = signature[1]

    # Webhook地址
    # access_token = "9dd2d1113f16858ccc815354b8ed9e83e39c051102aa666727a4643e68961e8c"

    url = "https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}".format(token, timestamp, sign)
    headers = {
        "Content-Type": "application/json"
    }

    body = {
        "msgtype": "text",
        "text": {"content": content},
        "at": {"atMobiles": atMobiles, "isAtAll": isAtAll}
    }

    res = requests.post(url=url, headers=headers, data=json.dumps(body))

# dingtalk_robot('亚马逊的文件爬取出错了',['18682156942'],False)
