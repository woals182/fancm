import pandas as pd
import requests
from selenium import webdriver
import time
from selenium.webdriver.common.by import By 
from bs4 import BeautifulSoup 
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from datetime import datetime

df = pd.read_csv('africa_bj_id.csv')

options = Options()
options.set_preference('intl.accept_languages', 'ko')
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35")

driver = webdriver.Firefox(options=options)

now = datetime.now().strftime("%Y-%m-%d")

africa = {
    "bj_id" : [],
    "favorite" : [], # 애청자
    "subscriber" : [], # 구독자
    "fanclub" : [], # 팬클럽
    "supporter" : [], # 서포터
    "accumulation_user" : [], # 누적유저
    "accumulation_up" : [], # 누적Up
    "today_up" : [], # 오느르이 Up
    "accumulation_visitor" : [], # 누적 방문 수
    "today_visitor" : [], # 오늘 방문 수
    "total_broadcating_time" : [], # 총 방송시간
    "recent_broadcating_date" : [], # 최근 방송일
    "publish_channel" : [], # 개설일 
    "get_date" : []
    }


for bj_id in df['bj_id']:
    try:
        url=f"https://bj.afreecatv.com/{bj_id}"
        time.sleep(2)    
        driver.get(url)
        
        button_info = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//span[contains(text(),"방송국 정보")]')))
        button_info.click()

        time.sleep(2)
        html = driver.page_source
        time.sleep(2)
        soup = BeautifulSoup(html,'html.parser')
        time.sleep(1)
        contents1 = driver.find_element(By.XPATH,'//*[@id="bs-contents"]/article[1]/section[2]/div/div[1]/div[1]/div/div/div[2]')
        time.sleep(2)

        data=contents1.get_attribute('innerText')
        lines = data.splitlines()
        print(bj_id)
        print(lines)
        
        africa['bj_id'].append(bj_id)
        africa['favorite'].append(lines[-13].split()[2].replace(",",'').replace("명",''))
        africa['subscriber'].append(lines[-12].split()[2].replace(",",'').replace("명",''))
        africa['fanclub'].append(lines[-11].split()[2].replace(",",'').replace("명",''))
        africa['supporter'].append(lines[-10].split()[2].replace(",",'').replace("명",''))
        africa['accumulation_user'].append(lines[-7].split()[2].replace(",",'').replace("명",''))
        africa['accumulation_up'].append(lines[-6].split()[3].replace(",",''))
        africa['today_up'].append(lines[-5].split()[2].replace(",",''))
        africa['accumulation_visitor'].append(lines[-4].split()[3].replace(",",''))
        africa['today_visitor'].append(lines[-3].split()[3].replace(",",''))
        africa['total_broadcating_time'].append(lines[-8].split()[3].replace(",",'').replace("시간",''))
        africa['recent_broadcating_date'].append(lines[-2].split()[2].replace(".",'-'))
        africa['publish_channel'].append(lines[-9].split()[2].replace(".",'-'))
        africa['get_date'].append(now)
        time.sleep(1)

        # print('최근 방송일 :',lines[-2].split()[2].replace(".",'-'))
        # print('오늘 방문 수 : ',lines[-3].split()[3].replace(",",''))
        # print('누적 방문 수 : ',lines[-4].split()[3].replace(",",''))
        # print('오늘 up 수 : ',lines[-5].split()[2].replace(",",''))
        # print('누적 up 수 : ',lines[-6].split()[3].replace(",",''))
        # print('누적 유저 : ',lines[-7].split()[2].replace(",",'').replace("명",''))
        # print('총 방송시간 : ',lines[-8].split()[3].replace(",",'').replace("시간",''))
        # print('방송국개설일 : ',lines[-9].split()[2].replace(".",'-'))
        # print('서포터 : ',lines[-10].split()[2].replace(",",'').replace("명",''))
        # print('팬클럽 : ',lines[-11].split()[2].replace(",",'').replace("명",''))
        # print('구독팬 : ',lines[-12].split()[2].replace(",",'').replace("명",''))
        # print('애청자 : ', lines[-13].split()[2].replace(",",'').replace("명",''))


    except:
        print('에러 발생')
        time.sleep(2)

    finally:
        df1 = pd.DataFrame(africa)
        df1.to_csv('/africa_data.csv',encoding='utf-8')

driver.quit()
