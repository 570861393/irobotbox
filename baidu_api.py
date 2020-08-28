from aip import AipOcr


def _get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()


# 调用百度图片识别
def baidu_discern(filename):
    """ 你的 APPID AK SK """
    APP_ID = '22095933'
    API_KEY = 'RmtKkhR6Etk2hVqGav0Yap4n'
    SECRET_KEY = 'DKFN4dhX3SUVlKgwt12HCLDltMUrGxZh'

    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)

    image = _get_file_content(filename)

    """ 调用网络图片文字识别, 图片参数为本地图片 """
    ret = client.webImage(image)
    words = ret.get('words_result')
    print(words)
    if words:
        return words[0]['words'].replace(' ','')
    else:
        return ''

