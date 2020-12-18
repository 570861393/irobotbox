import random
import time
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from baidu_api import baidu_discern
from PIL import Image
from put_task import *


def verification_code(driver):
    """识别验证码"""
    # 截取屏幕内容
    driver.save_screenshot("./image/page.png")
    ran = Image.open("./image/page.png")
    # 获取验证码位置，截取保存验证码
    img_element = driver.find_element_by_id("captchaImage")
    # 获取验证码坐标
    location = img_element.location
    # 获取验证码长度
    size = img_element.size
    # 定位验证码；参数：左上、右下
    code_range = (
        int(location['x']), int(location['y']), int(location['x'] + size['width'] + 1),
        int(location['y'] + size['height']))
    # 截取验证码保存
    ran.crop(code_range).save('./image/verification.png')
    result = baidu_discern("./image/verification.png")
    return result

def do_login():
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-extensions')  # 启动无痕界面
    options.add_argument('--start-maximized')  # 设置浏览器窗口最大化
    # options.add_argument('--headless')  # 浏览器不提供可视化页面
    options.add_argument('--disable-gpu')  # 禁用GPU加速
    options.add_argument('window-size=1920x3000')  # 指定浏览器分辨率
    options.add_argument('--no-sandbox')  # 添加沙盒模式
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options, executable_path='./chromedriver')
    """登录验证"""
    while True:
        driver.refresh()
        time.sleep(3)
        # 输入账号密码
        url = "http://wtkj.irobotbox.com/Manager/Index.aspx"
        driver.get(url)
        inp = driver.find_element_by_id("TextCustomerID")
        time.sleep(random.random() * 2)
        inp.send_keys(1837)
        driver.find_element_by_id("TextAdminName").send_keys("data009")
        time.sleep(random.random() * 2)
        driver.find_element_by_id("TextPassword").send_keys("data009009")
        time.sleep(random.random() * 2)
        code = verification_code(driver)
        print(code)
        # 输入验证码，点击登录
        verificode=driver.find_element_by_id("txtValidate")
        verificode.send_keys(code)
        print('输入验证码完成')
        time.sleep(3)
        driver.find_element_by_id("submit").click()
        time.sleep(5)
        # 检查是否登录成功
        try:
            text = driver.switch_to.alert.text
            time.sleep(1)
            if '验证码错误' in text:
                alert1 = driver.switch_to.alert
                alert1.accept()
                continue
        except:
            # 检查是否登录成功
            driver.switch_to.frame('frmain')
            login = driver.find_element_by_id('dvBody-Main')
            if login.is_enabled():
                print("登录成功！")
                break
            else:
                continue

    while True:
        driver.refresh()  # 刷新网页内容
        time.sleep(2)
        try:
            selenium_cookies = driver.get_cookies()
            cookies = {}
            for cookie in selenium_cookies:
                cookies[cookie['name']] = cookie['value']
            driver.switch_to.frame('frmain')
            # 移动鼠标到指定位置，perform()执行所有存储的操作（顺序被触发）
            ActionChains(driver).move_to_element(driver.find_element_by_xpath('//*[@id="CN_15"]/span')).perform()
            # 显示等待相应的元素出现（最长等待10s）,后执行点击动作
            sub = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="SubMenu_356"]')))
            sub.click()
            driver.switch_to.frame('iframeSubMenu')
            # print(driver.page_source)
            countrys_xpath = driver.find_element_by_xpath('//input[@id="ddlOrderSource"]')
            time.sleep(3)
            country_list = countrys_xpath.get_attribute('data-options').lstrip('data:')
            country_source = eval(country_list)[0]
            time.sleep(3)
            return cookies,str(country_source)
        except:
            continue  # 重新进行登录
        finally:
            driver.quit()







