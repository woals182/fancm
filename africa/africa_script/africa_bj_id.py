import pandas as pd
import requests
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

df = pd.read_csv('user_name.csv')

options = Options()
options.set_preference('intl.accept_languages', 'ko')
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35")

driver = webdriver.Firefox(options=options)

driver.implicitly_wait(10)

data = {
    "fancim_name" : [],
    "bj_name" : [],
    "bj_id" : []
    }

for fancim_name in df['fancim_name'][:]:
    try:
        url = f'https://www.afreecatv.com/total_search.html?szLocation=main&szSearchType=total&szKeyword={fancim_name}'
        driver.get(url)
        time.sleep(2)
        html = driver.page_source
        time.sleep(2)
        soup = BeautifulSoup(html,'html.parser')

        name = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME,'nick')))
        print(name.text)
        time.sleep(1)
        bj_id = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME,'id')))
        print(bj_id.text)
        time.sleep(1)


        data["fancim_name"].append(fancim_name)
        data["bj_name"].append(name.text)
        data["bj_id"].append(bj_id.text)
        time.sleep(1)

    except:
        print(f'{fancim_name}은 없네요.')

    finally:
        df1 = pd.DataFrame(data)
        df1.to_csv('africa_bj_id.csv',encoding='utf-8')

driver.quit()
