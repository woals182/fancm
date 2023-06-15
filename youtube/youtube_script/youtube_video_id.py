import pandas as pd
import requests
from selenium import webdriver
import time
from selenium.webdriver.common.by import By 
from bs4 import BeautifulSoup 
from selenium.webdriver.firefox.options import Options

path = '파일 위치'

df = pd.read_csv(f'{path}/youtube_name.csv')

options = Options()
options.set_preference('intl.accept_languages', 'ko')
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35")

driver = webdriver.Firefox(options=options)

data = {
    "channel_name" : [],
    "video_id" : []
}

for name in df['channel_name']:
    try:
        url=f"https://www.youtube.com/{name}/videos"
    
        driver.get(url)
        
        time.sleep(2)
        html = driver.page_source
        time.sleep(2)
        soup = BeautifulSoup(html,'html.parser')
        
        contents = driver.find_element(By.ID,'video-title-link')
        video_id = contents.get_attribute('href')[32:]
        print(video_id)
        time.sleep(2)
        data['channel_name'].append(name)
        data['video_id'].append(video_id)
        time.sleep(2)

    except:
        time.sleep(2)
        data['channel_name'].append(name)
        data['video_id'].append(None)
    
    finally:
        df1 = pd.DataFrame(data)
        df1.to_csv(f'{path}/youtube_video_id.csv',encoding='utf-8')

driver.quit()

