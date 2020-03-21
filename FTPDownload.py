import time
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from RediProcessor import FILE_ORIGIN

def ftp_download():
    # service = Service(FILE_ORIGIN + '/chromedriver')
    # service.start()
    driver = webdriver.Chrome(FILE_ORIGIN + 'Automations/chromedriver.exe')

    driver.get('https://tdsecurities.ftptoday.com/')

    # username = driver.find_element_by_xpath("/html/body[@class='exterior']/div[@id='exterior-wrap']/div[@id='exterior-content']/div[@id='exterior-middle']/div/div[@class='q-form q-form-vertical']/form[@id='login']/div[@class='q-form-controls']/div[@id='u-wrap']/div[@class='q-form-control']/input[@id='u']")

    username = driver.find_element_by_id("u")
    password = driver.find_element_by_id("p")
    login_botton = driver.find_element_by_css_selector("#login-q-mode-login")
    username.send_keys("user_rosalind")
    password.send_keys("34pr!me57")
    login_botton.click()

    time.sleep(1)

    reports_folder = driver.find_element_by_xpath("/html/body/div[2]/div[2]/div/div[2]/div/div[5]/div/div/div/form/div/table/tbody/tr[2]/td[2]/a")
    reports_folder.click()
    time.sleep(3)

    sort = driver.find_element_by_xpath("/html/body/div[2]/div[2]/div/div[2]/div/div[5]/div/div/div/form/div/table/thead/tr/th[5]/a")
    sort.click()
    time.sleep(3)

    sort = driver.find_element_by_xpath("/html/body/div[2]/div[2]/div/div[2]/div/div[5]/div/div/div/form/div/table/thead/tr/th[5]/a")
    sort.click()
    time.sleep(1)

    for i in range(2, 10):
        file = driver.find_element_by_xpath("/html/body/div[2]/div[2]/div/div[2]/div/div[5]/div/div/div/form/div/table/tbody/tr[{}]/td[2]/a".format(i))
        file.click()
        time.sleep(1)

    driver.quit()


