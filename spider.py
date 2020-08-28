# -*- coding: utf-8 -*-
# @Time    : 2019/08/05
# @Author  : coolchen
import traceback
from setting import *
import re
from get_cookie import do_login
import time
from urllib.request import urlretrieve
from dingding_notice import dingtalk_robot
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

today = datetime.date.today()-datetime.timedelta()
yesterday = datetime.date.today()-datetime.timedelta(days=1)
three_days_ago = datetime.date.today()-datetime.timedelta(days=3)
four_days_ago = datetime.date.today()-datetime.timedelta(days=4)

daytime = datetime.datetime.now().strftime('%Y-%m-%d')
pwd1 = os.getcwd()+"\\Files\\"+daytime+"\\FBADailyInventoryHistoryReport"
pwd2 = os.getcwd()+"\\Files\\"+daytime+"\\FBAManageInventory"
pwd3 = os.getcwd()+"\\Files\\"+daytime+"\\FBAReceivedInventoryReport"
pwd4 = os.getcwd()+"\\Files\\"+daytime+"\\FlatFileAllOrdersReportbyLastUpdate"
# print(pwd)
# 文件路径
word_name1 = os.path.exists(pwd1)
word_name2 = os.path.exists(pwd2)
word_name3 = os.path.exists(pwd3)
word_name4 = os.path.exists(pwd4)

if not word_name1:
    os.makedirs(pwd1)
if not word_name2:
    os.makedirs(pwd2)
if not word_name3:
    os.makedirs(pwd3)
if not word_name4:
    os.makedirs(pwd4)

class Myspider(object):
    def __init__(self):
        self.num = 0
        self.thread_num = 1 #多线程的个数
        self.keynum = 0
        self.browser_list = {}

        cookies = do_login()
        cookiestr = ''
        for cookie in cookies:
            str1 = cookie + '=' + cookies[cookie] + '; '
            cookiestr += str1
        self.headers = {
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

    def create_sql(self,column_item, table_name):
        column = ' ('
        values = ' ('
        for i in column_item:
            if str(column_item[i]) == 'None':
                continue
            column += i + ","
            one_word = re.sub("'", "\"", str(column_item[i]).replace('\n', '').replace('\r', '').replace('\t', '').replace('\\', '').replace('<em>', ''))
            values += repr(one_word) + ","
        column = column[:-1] + ',insert_time'
        values = values[:-1] + ',CURRENT_TIMESTAMP'
        sql = 'insert into ' + table_name + column.lower() + ') values' + values + ');'
        return sql

    # 清除数据中的脏乱字符
    def replaceall(self,word):
        return word.replace('\n','').replace(' ','').replace('\t','').replace('\xa0','').replace('\u3000','')

    # 放入任务到redis用于多线程
    def redis_put_task1(self):
        # ② FBA Daily Inventory History Report  白天可运行
        list2 = ["{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'54000001,645,81,658,386,830,456,112000001,737,853','reporttype':'36','typename':'FBA Daily Inventory History Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'84,91,848,891,754,320,807,818,881,825','reporttype':'36','typename':'FBA Daily Inventory History Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'306,740,840,826,742,878,834,837,768,54','reporttype':'36','typename':'FBA Daily Inventory History Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'814,310,461,308,879,739,883,888,889','reporttype':'36','typename':'FBA Daily Inventory History Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'538,836,838,760,82,89,666,816,427','reporttype':'36','typename':'FBA Daily Inventory History Report'}"]
        # ③ FBA Received Inventory Report   白天可运行
        list3 = ["{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'54000001,645,81,658,386,830,456,112000001,737,853','reporttype':'24','typename':'FBA Received Inventory Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'84,91,848,891,754,320,807,818,881,825','reporttype':'24','typename':'FBA Received Inventory Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'306,740,840,826,742,878,834,837,768,54','reporttype':'24','typename':'FBA Received Inventory Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'814,310,461,308,879,739,883,888,889','reporttype':'24','typename':'FBA Received Inventory Report'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(four_days_ago) + "','enddate':'" + str(today) + "','osid':'538,836,838,760,82,89,666,816,427','reporttype':'24','typename':'FBA Received Inventory Report'}",]
        # TODO 生成任务放入到redis中开始进行生成报告
        # list2+list3为白天跑，list1+list4为晚上跑
        for i in list2+list3:
            redis_conn.rpush(redis_task,str(i))
        errornum = 0
        while redis_conn.llen(redis_task) != 0:
            item2 = redis_conn.rpoplpush(redis_task, redis_task_backup)
            data = eval(item2.strip('\n'))
            print(data)
            try:
                url = 'http://gg55.irobotbox.com/ASHX/irobotbox/AmazonReportCentre.ashx'
                print(url)
                # proxies = {'https': "http://" + str(self.browser_list[thread_num]), }
                res = requests.post(url=url, headers=self.headers, timeout=10, data=data, verify=False)
                print(res.text)
                if res.text == 'ok':
                    print('请求成功')
                    time.sleep(30)
                    redis_conn.lrem(redis_task_backup, 0, item2)
                else:
                    errornum += 1
                    print('出错次数为' + str(errornum))
                    redis_conn.lrem(redis_task_backup, 0, item2)
                    redis_conn.lpush(redis_task, item2)
                    if errornum > 20:
                        dingtalk_robot('赛盒亚马逊---放入白天的任务过程中出错', ['18682156942'], False)
                        os._exit(0)
            except:
                errornum += 1
                print('出错次数为' + str(errornum))
                if errornum > 20:
                    dingtalk_robot('赛盒亚马逊---放入任务过程中出错', ['18682156942'], False)
                    os._exit(0)
                traceback.print_exc()
                print('出现问题，任务放回')
                redis_conn.lrem(redis_task_backup, 0, item2)
                redis_conn.lpush(redis_task, item2)
                time.sleep(5)
                continue
        dingtalk_robot('白天的任务已经放入完毕', ['18682156942'], False)

    def redis_put_task2(self):
        errornum = 0
        # ① Flat File All Orders Reportby Last Update  晚上8点后运行
        list1 = ["{'action':'SaveAmazonReportTask','begindate':'" + str(three_days_ago) + "','enddate':'" + str(
            today) + "','osid':'54000001,645,81,658,386,830,456,112000001,737,853','reporttype':'9','typename':'Flat File All Orders Reportby Last Update'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(three_days_ago) + "','enddate':'" + str(
                     today) + "','osid':'84,91,848,891,754,320,807,818,881,825','reporttype':'9','typename':'Flat File All Orders Reportby Last Update'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(three_days_ago) + "','enddate':'" + str(
                     today) + "','osid':'306,740,840,826,742,878,834,837,768,54','reporttype':'9','typename':'Flat File All Orders Reportby Last Update'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(three_days_ago) + "','enddate':'" + str(
                     today) + "','osid':'814,310,461,308,879,739,883,888,889','reporttype':'9','typename':'Flat File All Orders Reportby Last Update'}",
                 "{'action':'SaveAmazonReportTask','begindate':'" + str(three_days_ago) + "','enddate':'" + str(
                     today) + "','osid':'538,836,838,760,82,89,666,816,427','reporttype':'9','typename':'Flat File All Orders Reportby Last Update'}"]
        # ④ FBA Manage Inventory  晚上8点后运行
        list4 = [
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'54000001,645,81,658,386,830,456,112000001,737,853','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'84,91,848,891,754,320,807,818,881,825','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'306,740,840,826,742,878,834,837,768,54','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'814,310,461,308,879,739,883,888,889','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'538,836,838,760,82,89,666,816,427','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'892,753,321,819,882,307,749,841,821,885','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'835,877,820,55,815,319,462,309,880,750','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'884,539,839,761,83,90,667,817,804','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'41000001,734,717,871,867,855,85,92,849','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'212000001,874,736,870,857,88,95,851','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'38000001,733,872,868,854,86,93,847','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'112000001,737,853,84,91,848','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'53000001,735,873,869,856,87,94,824,850','reporttype':'20','typename':'FBA Manage Inventory'}",
            "{'action':'SaveAmazonReportTask','begindate':'" + str(today) + "','enddate':'" + str(
                today) + "','osid':'76000001,876,875,858,743,852','reporttype':'20','typename':'FBA Manage Inventory'}",
        ]
        for y in list1 + list4:
            redis_conn.rpush(redis_task, str(y))
        while redis_conn.llen(redis_task) != 0:
            item2 = redis_conn.rpoplpush(redis_task, redis_task_backup)
            data = eval(item2.strip('\n'))
            print(data)
            try:
                url = 'http://gg55.irobotbox.com/ASHX/irobotbox/AmazonReportCentre.ashx'
                print(url)
                # proxies = {'https': "http://" + str(self.browser_list[thread_num]), }
                res = requests.post(url=url, headers=self.headers, timeout=10, data=data, verify=False)
                print(res.text)
                if res.text == 'ok':
                    print('请求成功')
                    time.sleep(30)
                    redis_conn.lrem(redis_task_backup, 0, item2)
                else:
                    errornum += 1
                    print('出错次数为' + str(errornum))
                    redis_conn.lrem(redis_task_backup, 0, item2)
                    redis_conn.rpush(redis_task, item2)
                    if errornum > 10:
                        dingtalk_robot('赛盒亚马逊---放入晚上的任务过程中出错', ['18682156942'], False)
                        os._exit(0)
            except:
                errornum += 1
                print('出错次数为' + str(errornum))
                if errornum > 10:
                    dingtalk_robot('赛盒亚马逊---放入任务过程中出错', ['18682156942'], False)
                    os._exit(0)
                traceback.print_exc()
                print('出现问题，任务放回')
                redis_conn.lrem(redis_task_backup, 0, item2)
                redis_conn.rpush(redis_task, item2)
                time.sleep(5)
                continue
        dingtalk_robot('晚上的任务已经放入完毕', ['18682156942'], False)

    def down_url(self):
        print('等待报告生成中...')
        # TODO 开始下载报告
        t = int(round(time.time() * 1000000))
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        print(yesterday)
        page = 0
        check = 0
        while True:
            page += 1
            time.sleep(1)
            try:
                url = 'http://gg55.irobotbox.com/IrobotBox/ReportCentre/AmazonReportOut.aspx?action=InitData&pageIndex={}&type=&cate=&osid=&beginDate=&endDate=&_={}'.format(str(page), str(t))
                res = requests.get(url=url, headers=self.headers, timeout=10)
            except:
                time.sleep(3)
                page -= 1
                continue
            if check == 1:
                print('筛选完成')
                break
            for down_item in res.json()['Data']:
                print(down_item)
                AddTime = datetime.datetime.strptime(down_item['AddTime'], '%Y-%m-%d %H:%M:%S')
                nowtime = datetime.datetime.now().strptime(show_time(), '%Y-%m-%d %H:%M:%S')
                one_hour = datetime.datetime.now().strptime('1:00:00', '%H:%M:%S')
                three_hour = datetime.datetime.now().strptime('6:00:00', '%H:%M:%S')
                time_del = (nowtime - AddTime).total_seconds()
                # 3600为一小时 ，86400为一天
                if time_del > 3600 * 5:
                    check = 1
                    break
                AdminName = down_item['AdminName']
                TaskLog = down_item['TaskLog']
                FilePath = down_item['FilePath']
                if '报告下载成功' in TaskLog and AdminName == '爬虫':
                    down_item['downloadurl'] = 'http://gg55.irobotbox.com/' + FilePath.replace('至', '%E8%87%B3')
                    redis_conn.rpush(redis_downloadurl, str(down_item))

    # 爬取核心代码
    def spider_start(self):
        # TODO 下载文件到本地
        down_error = 0
        down_num = 0
        while redis_conn.llen(redis_downloadurl) != 0:
            # time.sleep(2)
            orginal_item = redis_conn.rpoplpush(redis_downloadurl, redis_downloadurl_backup)
            url_item = eval(orginal_item)
            down_url = url_item['downloadurl']
            OrderSourceName = url_item['OrderSourceName']
            BuildTime = url_item['BuildTime'].replace(':','-')
            TaskTitle = url_item['TaskTitle']
            TaskLog = url_item['TaskLog']
            if '发送报告请求成功' in TaskLog:
                dingtalk_robot(str(url_item),['18682156942'],False)
            dir = os.path.abspath('./Files/{}/'.format(str(daytime))+TaskTitle)
            #  特殊替换
            if 'Dreambig' in OrderSourceName:
                OrderSourceName = OrderSourceName.replace('Dreambig','SnugMax')
            work_path = os.path.join(dir, BuildTime+'_'+TaskTitle+'_'+OrderSourceName+'.txt')
            try:
                urlretrieve(down_url, work_path)
                down_num += 1
                redis_conn.lrem(redis_downloadurl_backup, 0, orginal_item)
            except:
                traceback.print_exc()
                down_error += 1
                redis_conn.lrem(redis_downloadurl_backup, 0, orginal_item)
                redis_conn.lpush(redis_downloadurl, orginal_item)
                if down_error > 50:
                    dingtalk_robot('赛盒亚马逊---下载文件的过程中出错,出错次数超过50次了', ['18682156942'], False)
                    os._exit(0)
        dingtalk_robot('一共下载文件数为：{}'.format(str(down_num)), ['18682156942'], False)

    # 对文件进行解析，存入到数据库
    def file_to_four(self):
        for path in [pwd1,pwd2,pwd3,pwd4]:
            files = os.listdir(path=path)
            file_list = []
            for file in files:
                account = file.split('_')[-1].split('-')[1]
                country_export = file.split('_')[-1].split('-')[2].rstrip('.txt')
                export_time = file.split('_')[0]
                try:
                    data = pd.read_csv(path + '/' + file, delimiter='\t', encoding=encod(path +'/'+file), low_memory=False,quoting=3)
                except:
                    print(file,'出错了')
                    break
                if data.shape[0] == 0:
                    continue
                data['account'] = account
                data['country_export'] = country_export
                data['export_time'] = export_time
                data['create_time'] = show_time()
                data['update_time'] = show_time()
                data['create_id'] = 'Spider'
                data['update_id'] = 'Spider'
                file_list.append(data)

            if 'FlatFileAllOrdersReportbyLastUpdate' in path:
                cloumn_list = ['account', 'country_export', 'amazon_order_id', 'merchant_order_id', 'purchase_date',
                               'last_updated_date',
                               'order_status', 'fulfillment_channel', 'sales_channel', 'order_channel', 'url',
                               'ship_service_level',
                               'product_name', 'sku_amazon', 'asin', 'item_status', 'quantity', 'currency', 'item_price',
                               'item_tax',
                               'shipping_price', 'shipping_tax', 'gift_wrap_price', 'gift_wrap_tax',
                               'item_promotion_discount',
                               'ship_promotion_discount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country',
                               'promotion_ids',
                               'is_business_order', 'purchase_order_number', 'price_designation', 'export_time',
                               'create_time',
                               'create_id', 'update_time', 'update_id']
                table_name = 'ods_amz_allod'
                rename_cloumn = {'gift-wrap-tax': 'gift_wrap_tax', 'order-status': 'order_status', 'ship-city': 'ship_city',
                                 'promotion-ids': 'promotion_ids', 'price-designation ': 'price_designation',
                                 'sales-channel': 'sales_channel', 'ship-country': 'ship_country',
                                 'shipping-tax': 'shipping_tax', 'product-name': 'product_name',
                                 'item-status': 'item_status', 'currency': 'currency', 'quantity': 'quantity',
                                 'is-business-order': 'is_business_order', 'amazon-order-id': 'amazon_order_id',
                                 'ship-state': 'ship_state', 'sku': 'sku_amazon', 'merchant-order-id': 'merchant_order_id',
                                 'item-tax': 'item_tax', 'purchase-order-number': 'purchase_order_number',
                                 'gift-wrap-price': 'gift_wrap_price', 'order-channel': 'order_channel',
                                 'fulfillment-channel': 'fulfillment_channel', 'item-price': 'item_price',
                                 'purchase-date': 'purchase_date', 'last-updated-date': 'last_updated_date',
                                 'ship-service-level': 'ship_service_level', 'asin': 'asin',
                                 'ship-postal-code': 'ship_postal_code', 'url': 'url', 'shipping-price': 'shipping_price',
                                 'item-promotion-discount': 'item_promotion_discount',
                                 'ship-promotion-discount': 'ship_promotion_discount', 'is-sold-by-ab ': 'is_sold_by_ab',
                                 'price-designation': 'price_designation', 'promotion-ids ': 'promotion_ids'}
            if 'FBADailyInventoryHistoryReport' in path:
                cloumn_list = ['account', 'country_export', 'snapshot_date', 'fnsku', 'sku_amazon', 'product_name',
                               'quantity',
                               'fulfillment_center_id', 'detailed_disposition', 'country', 'export_time', 'create_time',
                               'create_id', 'update_time', 'update_id']
                table_name = 'ods_amz_daily_inv'
                rename_cloumn = {'sku': 'sku_amazon', 'fnsku': 'fnsku', 'country': 'country',
                                 'snapshot-date': 'snapshot_date', 'detailed-disposition': 'detailed_disposition',
                                 'product-name': 'product_name', 'quantity': 'quantity',
                                 'fulfillment-center-id': 'fulfillment_center_id'}
            if 'FBAReceivedInventoryReport' in path:
                cloumn_list = ['account', 'country_export', 'received_date', 'fnsku', 'sku_amazon', 'product_name',
                               'quantity',
                               'fba_shipment_id', 'fulfillment_center_id', 'export_time', 'create_time', 'create_id',
                               'update_time', 'update_id']
                table_name = 'ods_amz_rec_inv'
                rename_cloumn = {'sku': 'sku_amazon', 'fnsku': 'fnsku', 'received-date': 'received_date',
                                 'fba-shipment-id': 'fba_shipment_id', 'product-name': 'product_name',
                                 'quantity': 'quantity', 'fulfillment-center-id': 'fulfillment_center_id',
                                 'FBA Shipment ID': 'fba_shipment_id', 'Quantity': 'quantity', 'FNSKU': 'fnsku',
                                 'Date': 'received_date', 'Merchant SKU': 'sku_amazon', 'Title': 'product_name',
                                 'FC': 'fulfillment_center_id'}
            if 'FBAManageInventory' in path:
                cloumn_list = ['account', 'country_export', 'sku_amazon', 'fnsku', 'asin', 'product_name', 'condition',
                               'your_price', 'mfn_listing_exists', 'mfn_fulfillable_quantity', 'afn_listing_exists',
                               'afn_warehouse_quantity', 'afn_fulfillable_quantity', 'afn_unsellable_quantity',
                               'afn_reserved_quantity', 'afn_total_quantity', 'per_unit_volume',
                               'afn_inbound_working_quantity',
                               'afn_inbound_shipped_quantity', 'afn_inbound_receiving_quantity', 'afn_researching_quantity',
                               'afn_reserved_future_supply', 'afn_future_supply_buyable', 'export_time', 'create_time',
                               'create_id', 'update_time', 'update_id']
                table_name = 'ods_amz_man_inv'
                rename_cloumn = {'afn-reserved-future-supply': 'afn_reserved_future_supply',
                                 'mfn-listing-exists': 'mfn_listing_exists', 'your-price': 'your_price',
                                 'afn-total-quantity': 'afn_total_quantity',
                                 'afn-researching-quantity': 'afn_researching_quantity', 'fnsku': 'fnsku',
                                 'afn-inbound-shipped-quantity': 'afn_inbound_shipped_quantity',
                                 'afn-inbound-working-quantity': 'afn_inbound_working_quantity',
                                 'afn-listing-exists': 'afn_listing_exists', 'per-unit-volume': 'per_unit_volume',
                                 'afn-unsellable-quantity': 'afn_unsellable_quantity', 'product-name': 'product_name',
                                 'afn-fulfillable-quantity': 'afn_fulfillable_quantity', 'sku': 'sku_amazon',
                                 'afn-reserved-quantity': 'afn_reserved_quantity',
                                 'mfn-fulfillable-quantity': 'mfn_fulfillable_quantity',
                                 'afn-warehouse-quantity': 'afn_warehouse_quantity',
                                 'afn-future-supply-buyable': 'afn_future_supply_buyable', 'asin': 'asin',
                                 'condition': 'condition',
                                 'afn-inbound-receiving-quantity': 'afn_inbound_receiving_quantity'}

            result = pd.concat(file_list)
            result.rename(columns=rename_cloumn, inplace=True)
            data2 = result[cloumn_list]
            print(data2.columns.values)
            print(data2.shape[0])
            pd.io.sql.to_sql(data2, table_name, con=engine, if_exists='append', index=False)

    def file_to_only(self):
        user_name = "root"
        user_password = "Biadmin@123"
        database_ip = "10.0.1.73:3306"
        database_name = "vt_amz_inventory"
        database_all = "mysql+pymysql://" + user_name + ":" + user_password + "@" + database_ip + \
                       "/" + database_name + "?charset=utf8mb4"
        engine = create_engine(database_all)
        files = os.listdir(path=pwd1)
        file_list = []
        for file in files:
            account = file.split('_')[-1].split('-')[1]
            country_export = file.split('_')[-1].split('-')[2].rstrip('.txt')
            try:
                data = pd.read_csv(pwd1 + '/' + file, delimiter='\t', encoding=encod(pwd1 + '/' + file),low_memory=False, quoting=3)
            except:
                print(file, '出错了')
                break

            if data.shape[0] == 0:
                continue
            data['account'] = account
            data['country_export'] = country_export

            file_list.append(data)
        cloumn_list = ['account', 'country_export', 'country', 'snapshot_date', 'fnsku', 'sku_amazon',
                       'product_name', 'quantity',
                       'fulfillment_center_id', 'detailed_disposition']
        table_name = 'fba_inv_day_import'
        rename_cloumn = {'sku': 'sku_amazon', 'fnsku': 'fnsku', 'country': 'country',
                         'snapshot-date': 'snapshot_date', 'detailed-disposition': 'detailed_disposition',
                         'product-name': 'product_name', 'quantity': 'quantity',
                         'fulfillment-center-id': 'fulfillment_center_id'}

        result = pd.concat(file_list)
        result.rename(columns=rename_cloumn, inplace=True)
        data3 = result[cloumn_list]

        print(data3.columns.values)
        print('----------------------')
        print(data3)

        pd.io.sql.to_sql(data3, table_name, con=engine, if_exists='append', index=False)

    def run_spider(self):
        # 6点开始生成白天的请求报告
        # self.redis_put_task1()
        # # 8点开始生成晚上的请求报告
        # self.redis_put_task2()
        # 下载所有报告生成后的链接
        self.down_url()
        # 下载所有文件存入本地
        self.spider_start()
        # 晚上11点半开始导入数据到4张表
        self.file_to_four()
        self.file_to_only()

if __name__ == '__main__':
    Myspider().run_spider()