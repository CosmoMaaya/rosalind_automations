import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

service = Service('/home/yurikaendou/文档/rosalind/chromedriver')
service.start()
driver = webdriver.Remote(service.service_url)

driver.get('https://tdsecurities.ftptoday.com/');

username = driver.findElement(By.xpath(
        "/html/body[@class='exterior']/div[@id='exterior-wrap']/div[@id='exterior-content']/div[@id='exterior-middle']/div/div[@class='q-form q-form-vertical']/form[@id='login']/div[@class='q-form-controls']/div[@id='u-wrap']/div[@class='q-form-control']/input[@id='u']"));
time.sleep(5) # Let the user actually see something!
driver.quit()