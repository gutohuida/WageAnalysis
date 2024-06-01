# Standard library imports
import os
import time
from datetime import datetime

# Third-party library imports
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Application-specific imports
from prefect_dags.common.countries import (
    AF_COUNTRIES,
    ASIAN_COUNTRIES,
    EU_COUNTRIES,
    NA_COUNTRIES,
    OCE_COUNTRIES,
    SA_COUNTRIES,
)

load_dotenv()

## DECLARES ##
ENV = os.environ.get('ENV')
FULL_LOAD = os.environ.get('FULL_LOAD')

DAY_OF_WEEK = datetime.today().weekday()

if DAY_OF_WEEK == 6:
    REGION = 'EU'
elif DAY_OF_WEEK == 0:
    REGION = 'NA'
elif DAY_OF_WEEK == 1:
    REGION = 'SA'
elif DAY_OF_WEEK == 2:
    REGION = 'ASIA'
elif DAY_OF_WEEK == 3:            
    REGION = 'AF'
else:
    REGION = 'GLOBAL'

MAIN_URL = os.environ.get('MAIN_URL')

PAGE_TIME_OUT = os.environ.get('PAGE_TIME_OUT')

DB_USER = os.environ.get('DB_USER')
DB_PSSWRD = os.environ.get('DB_PSSWRD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

CONNECTION_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PSSWRD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

if REGION == 'EU':
    COUNTRIES = EU_COUNTRIES
elif REGION == 'NA':
    COUNTRIES = NA_COUNTRIES + OCE_COUNTRIES
elif REGION == 'SA':
    COUNTRIES = SA_COUNTRIES
elif REGION == 'ASIA':
    COUNTRIES = ASIAN_COUNTRIES
elif REGION == 'AF':
    COUNTRIES = AF_COUNTRIES  
elif REGION == 'GLOBAL' and FULL_LOAD:
    COUNTRIES = EU_COUNTRIES + NA_COUNTRIES + OCE_COUNTRIES + SA_COUNTRIES + ASIAN_COUNTRIES + AF_COUNTRIES
       

if ENV == 'DEV':
    COUNTRY = "Portugal"      
    
driver = webdriver.Chrome()
driver.maximize_window()

driver.get(MAIN_URL)

accept_cookies = driver.find_element(By.ID,'accept-choices')

accept_cookies.click()
time.sleep(2)

country = driver.find_element(By.XPATH, f"//a[contains(text(), '{COUNTRY}')]")

print(country.text)
try:
    country.click()
except Exception as e:
    print(e)

page = driver.page_source
soup = BeautifulSoup(page, 'html.parser')

summary_div = soup.find('div', class_='seeding-call table_color summary limit_size_ad_right padding_lower other_highlight_color')

summary_list = summary_div.find('ul')

summary_list

summary = summary_list.find_all('li')
summary_dict = {}
summary_dict['family_of_4'] = summary[0].text
summary_dict['single'] = summary[1].text
summary_dict['cost_comparison'] = summary[2].text
summary_dict['rent_comparison'] = summary[3].text

summary_df = pd.DataFrame([summary_dict])

summary_df.head()

discretionary_values_table = soup.find('table', class_='data_wide_table new_bar_table')

discretionary_values_table

details_list = discretionary_values_table.find_all('tr')

details = []
for element in details_list:
    detail_dict = {}
    if element.find('th'):
        continue
        
    valid_details = element.find_all('td')
    detail_dict['country'] = COUNTRY
    detail_dict['city'] = None
    detail_dict['type'] = valid_details[0].text
    detail_dict['amount'] = valid_details[1].text
    detail_dict['range'] = valid_details[2].text
    
    details.append(detail_dict)

details_df = pd.DataFrame(details)

details_df.head(10)

postgresql_url = CONNECTION_URL
engine = create_engine(postgresql_url)

details_df.to_sql('country_expenses_detail', engine, schema='raw', if_exists='replace', index=False)

summary_df.to_sql('country_expenses', engine, schema='raw', if_exists='replace', index=False)
