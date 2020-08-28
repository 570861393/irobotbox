import requests
from setting import *
from get_cookie import do_login
import time
from urllib.request import urlretrieve

def download():
    cookies = do_login()
    cookiestr = ''
    for cookie in cookies:
        str1 = cookie + '=' + cookies[cookie] + '; '
        cookiestr += str1
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': cookiestr.rstrip('; '),
        'Host': 'gg55.irobotbox.com',
        'Referer': 'http://gg55.irobotbox.com/IrobotBox/ReportCentre/AmazonReportOut.aspx',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }
    t = int(round(time.time() * 1000000))
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    print(yesterday)
    page = 0
    check = 0
    while True:
        page+=1
        time.sleep(2)
        try:
            url = 'http://gg55.irobotbox.com/IrobotBox/ReportCentre/AmazonReportOut.aspx?action=InitData&pageIndex={}&type=&cate=&osid=&beginDate=&endDate=&_={}'.format(str(page),str(t))
            res = requests.get(url=url,headers=headers,timeout=10)
        except:
            time.sleep(3)
            page-=1
            continue
        if check == 1:
            print('筛选完成')
            break
        for i in res.json()['Data']:
            AddTime = datetime.datetime.strptime(i['AddTime'],'%Y-%m-%d %H:%M:%S')
            nowtime = datetime.datetime.now().strptime(show_time(),'%Y-%m-%d %H:%M:%S')
            one_hour = datetime.datetime.now().strptime('1:00:00','%H:%M:%S')
            two_hour = datetime.datetime.now().strptime('3:00:00','%H:%M:%S')
            twelve_hour = datetime.datetime.now().strptime('12:00:00','%H:%M:%S')
            time_del = datetime.datetime.now().strptime(str(nowtime-AddTime),'%H:%M:%S')
            print(nowtime - AddTime)
            if time_del>twelve_hour:
                check=1
                break
            AdminName =i['AdminName']
            FilePath = i['FilePath']
            TaskLog = i['TaskLog']
            downloadurl = 'http://gg55.irobotbox.com/'+FilePath
            if '报告下载成功' in TaskLog and AdminName =='爬虫' and one_hour < time_del < two_hour :
                print('符合条件')
                redis_conn.rpush(redis_downloadurl,downloadurl)
    while redis_conn.llen(redis_downloadurl) != 0:
        item = redis_conn.rpoplpush(redis_downloadurl,redis_error)
        print('还有任务')
        dir = os.path.abspath('./Files')
        url = item.replace('至', '%E8%87%B3')
        work_path = os.path.join(dir, url.split('/')[-2])
        # urlretrieve(url, work_path)

download()




















