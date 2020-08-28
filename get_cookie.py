import random
import time
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from baidu_api import baidu_discern
from PIL import Image


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
    options.add_argument('--start-maximized')  # 设置浏览器窗口最大化
    driver = webdriver.Chrome(options=options)
    """登录验证"""
    try:
        # 输入账号密码
        url = "http://gg55.irobotbox.com/Manager/Index.aspx"
        driver.get(url)
        inp = driver.find_element_by_id("TextCustomerID")
        time.sleep(random.random() * 2)
        inp.send_keys(1837)
        driver.find_element_by_id("TextAdminName").send_keys("data009")
        time.sleep(random.random() * 2)
        driver.find_element_by_id("TextPassword").send_keys("data009")
        time.sleep(random.random() * 2)
        code = verification_code(driver)
        print(code)
        # 输入验证码，点击登录
        verificode=driver.find_element_by_id("txtValidate")
        verificode.send_keys(code)
        print('输入验证码完成')
        driver.find_element_by_id("submit").click()
        # 检查是否登录成功
        time.sleep(5)
        print(driver.current_url)
        if driver.current_url == 'http://gg55.irobotbox.com/Manager/index.aspx':
            print('登录成功，开始获取cookie')
            selenium_cookies = driver.get_cookies()
            cookies={}
            for cookie in selenium_cookies:
                cookies[cookie['name']] = cookie['value']
            print(cookies)
            return cookies
        else:
            driver.refresh()
            print('进入第一层')
            time.sleep(1)
            do_login()

        # driver.switch_to.frame('frmain')
        # login = driver.find_element_by_id('dvBody-Main')
        # if login.is_enabled():
        #     print("登录成功！")

    except Exception as err:
        print(err)
        print('进入第二层')
        driver.refresh()  # 刷新网页内容
        time.sleep(2)
        do_login()  # 重新进行登录
    finally:
        driver.quit()





