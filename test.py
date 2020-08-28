import datetime
import os

daytime = datetime.datetime.now().strftime('%Y-%m-%d')
pwd1 = os.getcwd()+"\\"+daytime+"\\Files\\FBADailyInventoryHistoryReport"
pwd2 = os.getcwd()+"\\"+daytime+"\\Files\\FBAManageInventory"
pwd3 = os.getcwd()+"\\"+daytime+"\\Files\\FBAReceivedInventoryReport"
pwd4 = os.getcwd()+"\\"+daytime+"\\Files\\FlatFileAllOrdersReportbyLastUpdate"
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