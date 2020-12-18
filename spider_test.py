# -*- coding: utf-8 -*-
# @Time    : 2019/08/05
# @Author  : coolchen
import traceback
from setting import *
import re
from get_cookie import do_login
import time
from dingding_notice import dingtalk_robot
from urllib.request import urlretrieve
import pandas as pd
from put_task import *

today = datetime.date.today()-datetime.timedelta()
yesterday = datetime.date.today()-datetime.timedelta(days=1)
three_days_ago = datetime.date.today()-datetime.timedelta(days=3)
four_days_ago = datetime.date.today()-datetime.timedelta(days=4)

daytime = datetime.datetime.now().strftime('%Y-%m-%d')
pwd1 = SHARE_DIR + daytime + "//FBADailyInventoryHistoryReport"
pwd2 = SHARE_DIR + daytime + "//FBAManageInventory"
pwd3 = SHARE_DIR + daytime + "//FBAReceivedInventoryReport"
pwd4 = SHARE_DIR + daytime + "//FlatFileAllOrdersReportbyLastUpdate"

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
        cookies,country_source = do_login()
        put_task(country_source)
        print(cookies)
        cookiestr = ''
        for cookie in cookies:
            str1 = cookie + '=' + cookies[cookie] + '; '
            cookiestr += str1
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'close',
            'Cookie': cookiestr.rstrip('; '),
            'Host': 'wtkj.irobotbox.com',
            'Referer': 'http://wtkj.irobotbox.com/IrobotBox/ReportCentre/AmazonReportOut.aspx',
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
        errornum = 0
        # 下午2点的任务
        while redis_conn.llen(redis_task) != 0:
            item2 = redis_conn.rpoplpush(redis_task, redis_task_backup)
            data = eval(item2.strip('\n'))
            print(data)
            try:
                url = 'http://wtkj.irobotbox.com/ASHX/irobotbox/AmazonReportCentre.ashx'
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
                        dingtalk_robot('赛盒亚马逊---下午8点放入的任务过程中出错', ['18682156942'], False)
                        os._exit(0)
            except Exception as E:
                print('错误2:'+str(E))
                errornum += 1
                print('出错次数为' + str(errornum))
                if errornum > 20:
                    dingtalk_robot('赛盒亚马逊---下午8点放入的任务过程中出错', ['18682156942'], False)
                    os._exit(0)
                traceback.print_exc()
                print('出现问题，任务放回')
                redis_conn.lrem(redis_task_backup, 0, item2)
                redis_conn.lpush(redis_task, item2)
                time.sleep(5)
                continue
        dingtalk_robot('赛盒亚马逊---下午8点的任务已经放入完毕', ['18682156942'], False)

    def redis_put_task2(self):
        errornum = 0
        # 下午4点的任务
        while redis_conn.llen(redis_task2) != 0:
            item2 = redis_conn.rpoplpush(redis_task2, redis_task2_backup)
            data = eval(item2.strip('\n'))
            print(data)
            try:
                url = 'http://wtkj.irobotbox.com/ASHX/irobotbox/AmazonReportCentre.ashx'
                print(url)
                # proxies = {'https': "http://" + str(self.browser_list[thread_num]), }
                res = requests.post(url=url, headers=self.headers, timeout=10, data=data, verify=False)
                print(res.text)
                if res.text == 'ok':
                    print('请求成功')
                    time.sleep(30)
                    redis_conn.lrem(redis_task2_backup, 0, item2)
                else:
                    errornum += 1
                    print('出错次数为' + str(errornum))
                    redis_conn.lrem(redis_task2_backup, 0, item2)
                    redis_conn.rpush(redis_task2, item2)
                    if errornum > 10:
                        dingtalk_robot('赛盒亚马逊---下午6点放入的任务过程中出错', ['18682156942'], False)
                        os._exit(0)
            except Exception as E:
                print('错误3:'+str(E))
                errornum += 1
                print('出错次数为' + str(errornum))
                if errornum > 10:
                    dingtalk_robot('赛盒亚马逊---下午6点放入的任务出错', ['18682156942'], False)
                    os._exit(0)
                traceback.print_exc()
                print('出现问题，任务放回')
                redis_conn.lrem(redis_task2_backup, 0, item2)
                redis_conn.rpush(redis_task2, item2)
                time.sleep(5)
                continue
        dingtalk_robot('赛盒亚马逊---下午6点放入的任务已经放入完毕', ['18682156942'], False)

    def down_url(self):
        download_error_num=0
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
                url = 'http://wtkj.irobotbox.com/IrobotBox/ReportCentre/AmazonReportOut.aspx?action=InitData&pageIndex={}&type=&cate=&osid=&beginDate=&endDate=&_={}'.format(str(page), str(t))
                res = requests.get(url=url, headers=self.headers, timeout=10)
            except Exception as E:
                print('错误4:'+str(E))
                time.sleep(3)
                page -= 1
                continue
            if check == 1:
                print('筛选完成')
                break
            for down_item in res.json()['Data']:
                # print(down_item)
                AddTime = datetime.datetime.strptime(down_item['AddTime'], '%Y-%m-%d %H:%M:%S')
                cooltime = down_item['AddTime'].split(' ')[0]
                nowtime = datetime.datetime.now().strptime(show_time(), '%Y-%m-%d %H:%M:%S')
                time_del = (nowtime - AddTime).total_seconds()
                # 3600为一小时 ，86400为一天
                if time_del > 3600 * 9:
                    check = 1
                    break
                AdminName = down_item['AdminName']
                TaskLog = down_item['TaskLog']
                FilePath = down_item['FilePath']
                today_data = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                yesterday_data = time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400))
                if AdminName == '爬虫' and (TaskLog is None or '发送报告请求成功' in TaskLog):
                    print('异常报告')
                    download_error_num += 1
                elif '报告下载成功' in TaskLog and AdminName == '爬虫':
                    bulu_min = '{} 21:00:00'.format(yesterday_data)
                    bulu_min_time = datetime.datetime.strptime(bulu_min, "%Y-%m-%d %H:%M:%S")
                    bulu_max = '{} 14:00:00'.format(today_data)
                    bulu_max_time = datetime.datetime.strptime(bulu_max, "%Y-%m-%d %H:%M:%S")
                    shoudong_time = TaskLog.split('：')[0].replace('/','-')
                    bulude_time = datetime.datetime.strptime(shoudong_time, "%Y-%m-%d %H:%M:%S")
                    # 正常的报告
                    if shoudong_time > bulu_max:
                        down_item['downloadurl'] = 'http://wtkj.irobotbox.com/' + FilePath.replace('至', '%E8%87%B3')
                        redis_conn.rpush(redis_downloadurl, str(down_item))
                    # 补录的报告
                    elif bulu_min_time < bulude_time < bulu_max_time:
                        down_item['downloadurl'] = 'http://wtkj.irobotbox.com/' + FilePath.replace('至', '%E8%87%B3')
                        redis_conn.rpush(redis_downloadurl_error, str(down_item))
        dingtalk_robot('赛盒亚马逊--文件还未生成数量为{}，明天晚上进行补录'.format(str(download_error_num)), ['18682156942','18124772343'], False)

    # 爬取核心代码
    def spider_start(self):
        # TODO 下载文件到本地
        down_error = 0
        down_num = 0
        bulu = redis_conn.llen(redis_downloadurl_error)
        while redis_conn.llen(redis_downloadurl_error) !=0:
            downurl = redis_conn.rpop(redis_downloadurl_error)
            redis_conn.rpush(redis_downloadurl,downurl)
        while redis_conn.llen(redis_downloadurl) != 0:
            # time.sleep(2)
            orginal_item = redis_conn.rpoplpush(redis_downloadurl, redis_downloadurl_backup)
            url_item = eval(orginal_item)
            down_url = url_item['downloadurl']
            OrderSourceName = url_item['OrderSourceName']
            BuildTime = url_item['BuildTime'].replace(':','-')
            TaskTitle = url_item['TaskTitle']
            TaskLog = url_item['TaskLog']
            dir = '/mnt/工作/赛盒/Files/{}/'.format(str(daytime))+TaskTitle
            #  特殊替换
            if 'Dreambig' in OrderSourceName:
                OrderSourceName = OrderSourceName.replace('Dreambig','SnugMax')
            work_path = os.path.join(dir, BuildTime+'_'+TaskTitle+'_'+OrderSourceName+'.txt')
            try:
                urlretrieve(down_url, work_path)
                down_num += 1
                redis_conn.lrem(redis_downloadurl_backup, 0, orginal_item)
            except Exception as E:
                print('错误5:'+str(E))
                down_error += 1
                redis_conn.lrem(redis_downloadurl_backup, 0, orginal_item)
                redis_conn.lpush(redis_downloadurl, orginal_item)
                if down_error > 100:
                    dingtalk_robot('赛盒亚马逊---下载文件的过程中出错,出错次数超过50次了,需要处理', ['18682156942'], False)
                    os._exit(0)
        dingtalk_robot('一共下载文件数为：{},补录昨天的任务数为：{}'.format(str(down_num),str(bulu)), ['18682156942','18124772343'], False)

    def file_to_four(self):
        for path in [pwd1,pwd2,pwd3,pwd4]:
            files = os.listdir(path=path)
            file_list = []
            for file in files:
                account = file.split('_')[-1].split('-')[1]
                country_export = file.split('_')[-1].split('-')[2].rstrip('.txt')
                export_time = file.split('_')[0]
                try:
                    data = pd.read_csv(path + '/' + file, delimiter='\t', encoding=encod(path +'/'+file), low_memory=False,quoting=3,error_bad_lines=False)
                except Exception as E:
                    print('错误6:'+str(E))
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
            try:
                result = pd.concat(file_list)
            except:
                pass
            result.rename(columns=rename_cloumn, inplace=True)
            data2 = result[cloumn_list]
            print(data2.columns.values)
            print(data2.shape[0])
            try:
                # pd.io.sql.to_sql(data2, table_name, con=engine, if_exists='append', index=False)
                dingtalk_robot('赛盒亚马逊---数据导入\n{}新增数据总量为:{}'.format(table_name,str(data2.shape[0])), ['18682156942', '18124772343'], False)
            except Exception as E:
                print('错误7:'+str(E))
                dingtalk_robot('数据有点错误'+str(E), ['18682156942'], False)

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
            except Exception as E:
                print('错误8:'+str(E))
                dingtalk_robot('赛盒亚马逊---亚马逊文件解析失败\n出错文件为:{}\n报错信息为:{}'.format(str(file),str(E)), ['18682156942'], False)
                continue

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
        pd.io.sql.to_sql(data3, table_name, con=engine, if_exists='append', index=False)
        dingtalk_robot('赛盒亚马逊---库存数据导入数据总量为{}'.format(data3.shape[0]), ['18682156942','15017227189'], False)

    def run_spider(self):
        # 下午6点开始生成白天的请求报告
        self.redis_put_task2()
        time.sleep(3600*1.9)
        # 下午8点开始生成晚上的请求报告
        self.redis_put_task1()
        # 下载所有报告生成后的链接
        time.sleep(3600*3)
        self.down_url()
        # 下载所有文件存入本地
        self.spider_start()
        # 晚上11点半开始导入数据到4张表
        self.file_to_four()
        self.file_to_only()
        dingtalk_robot('赛盒亚马逊---下载文件已经全部入库完成,程序退出', ['18682156942'], False)

if __name__ == '__main__':
    try:
        Myspider().run_spider()
    except Exception as E:
        print('错误1:'+str(E))
        dingtalk_robot('赛盒亚马逊---爬虫文件出错了\n报错信息为:{}'.format(str(E)), ['18682156942'], False)
