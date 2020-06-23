# -*- encoding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import lxml
import csv
import uuid
import re

import threading
from queue import Queue
import time
from datetime import datetime
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import os

base_path = "your base path"
encoding = "utf-8"

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--disable-infobars")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sanbox")
chrome_options.add_argument("--hide-scrollbars")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("blink-settings=imagesEnable=false")

# 描述
# company:  需要搜索的公司/人物名字
# q_name:   用于名命输出文件

def crawler(company, q_name , browser_handle):
    baidu_url = r"https://www.baidu.com"
    wd ="intitle:" + company + " site:linkedin.com inurl:in"
    browser_handle.implicitly_wait(5)
    try:
        browser_handle.get("https://www.baidu.com/s?wd=%s"%wd)
    except Exception as e:
        print(e)
        pass
    soup = BeautifulSoup(browser_handle.page_source, "lxml")
    reg = re.compile("rsv_page=1")
    if(soup.find("div",class_="content_none") != None):
        return True
    pattern = re.compile('[-|–]')
    tmp = []
    tmp_error = []
    while(soup.find("div",class_="content_none") == None):
        for item in soup.find_all("div", class_="result c-container"):
            try:
                tmp.append({'suid': str(uuid.uuid4()).replace("-", ""),
                        'intitle': item.find("a").text,
                        'inbody': item.find('div', class_='c-abstract').text,
                        'site': item.find("a")['href'],
                        'postion': pattern.split(item.find("a").text)[1]})
            except IndexError:
                pass
        time.sleep(random.uniform(0.2, 0.9))
        try:
            next_page = soup.find("a",href = reg)['href']
            browser_handle.get(baidu_url + next_page)
            soup = BeautifulSoup(browser_handle.page_source, "lxml")
        except TypeError:
            print("已经没有下一页")
            break

    # 输出文件的路径
    filename=base_path + "\\directory\\ " + q_name + ".csv"
    error_log = base_path + "\\directory\\" + "error_log.csv"

    for num,item in enumerate(tmp):
        try:
            item['site']=requests.get(item['site'],allow_redirects=False,verify=False).headers['location']
            # time.sleep(random.uniform(0.2,0.5))
        except requests.exceptions.ConnectionError as e:
            tmp_error.append(item)
            print(str(num)+":"+item['site'])
            
    if(os.path.isfile(error_log)):
        with open( error_log,"a+",newline='',encoding="utf-8") as f:
            fieldnames = [
            'suid',
            'intitle',
            'site',
            'inbody',
            'postion',]
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            for i in tmp_error:
                writer.writerow({
                        'suid':i['suid'],
                        'intitle':i['intitle'],
                        'site':i['site'],
                        'inbody':i['inbody'],
                        'postion':i['postion']})
    else:
        with open( error_log,"a+",newline='',encoding="utf-8") as f:
            fieldnames = [
            'suid',
            'intitle',
            'site',
            'inbody',
            'postion',]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i in tmp_error:
                 writer.writerow({
                    'suid':i['suid'],
                    'intitle':i['intitle'],
                    'site':i['site'],
                    'inbody':i['inbody'],
                    'postion':i['postion']})
    
    if(os.path.isfile(filename)):
        with open(filename, "a+",newline='',encoding="utf-8") as f:
            fieldnames = [
            'suid',
            'intitle',
            'site',
            'inbody',
            'postion',]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            for i in tmp:
                if ('linkedin.com/in/' in i['site']):
                    writer.writerow({
                        'suid':i['suid'],
                        'intitle':i['intitle'],
                        'site':i['site'],
                        'inbody':i['inbody'],
                        'postion':i['postion']})
                else:
                    pass
    else:
        with open(filename, "a+",newline='',encoding="utf-8") as f:
            fieldnames = [
            'suid',
            'intitle',
            'site',
            'inbody',
            'postion',]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i in tmp:
                if ('linkedin.com/in/' in i['site']):
                    writer.writerow({
                    'suid':i['suid'],
                    'intitle':i['intitle'],
                    'site':i['site'],
                    'inbody':i['inbody'],
                    'postion':i['postion']})
                else:
                    pass
    
    return True

'''
webdriver要是跑的页面不多还好
要是一个driver跑久了就容易出现内存溢出的问题
一开始的想法是无论捕获到什么错误都关闭这个driver重新启动
然后发现事情没那么简单
最后采取笨方法
每个driver跑一定次数之后手动重启
实测下来效果还好 内存状态良好
'''
def worker(name, q):
    while not q.empty():
        driver = webdriver.Chrome(options=chrome_options)
        for i in range(1000):
            company_name = q.get()[0]
            print("开始爬取：" + company_name)
            try:
                crawler(company_name , name ,driver)
            except Exception as e:
                print(e)
                driver.quit()
                driver = webdriver.Chrome(options=chrome_options)
                continue
            time.sleep(1)
            driver.delete_all_cookies()
            q.task_done()
        driver.quit()
        
        
def producer(name, q):
    with open("base path name\\"+ name +".csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line in reader:
            q.put(line)
    q.join()
    


if __name__ == "__main__":
    q = Queue()

    # reader = threading.Thread(target=producer, args=('reader', q))
    # reader.start()
    p_treads = []

    for i in range(141,168):
        thread = threading.Thread(target=producer, args=(str(i), q))
        p_treads.append(thread)

    for i in p_treads:
        i.start()

    worker_threads = []

    for i in range(25):
        thread = threading.Thread(target=worker, args=(str(i), q))
        worker_threads.append(thread)

    for thread in worker_threads:
        thread.start()

    for i in p_treads:
         i.join()
    q.join()

    # print("进程结束" + datetime.now().strftime("%Y-%m-%d %H:%M"))
