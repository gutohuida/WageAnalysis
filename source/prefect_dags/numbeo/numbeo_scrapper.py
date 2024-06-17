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
from prefect import flow, task, get_run_logger

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

if FULL_LOAD == True:
    REGION = 'GLOBAL'  
elif DAY_OF_WEEK == 6:
    REGION = 'EU'
elif DAY_OF_WEEK == 0:
    REGION = 'NA'
elif DAY_OF_WEEK == 1:
    REGION = 'SA'
elif DAY_OF_WEEK == 2:
    REGION = 'ASIA'
elif DAY_OF_WEEK == 3:            
    REGION = 'AF' 

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
    COUNTRIES = NA_COUNTRIES + OCE_COUNTRIES    

@task
def init_driver():
    driver = webdriver.Chrome()
    return driver

@task    
def scrap_summary(soup, country):
    logging = get_run_logger()
    logging.info('Scrapping summary...')
    summary_dict = {}
    summary_div = soup.find('div', class_='seeding-call table_color summary limit_size_ad_right padding_lower other_highlight_color')
    summary_list = summary_div.find('ul')
    summary = summary_list.find_all('li')
    
    summary_dict['country'] = country
    summary_dict['city'] = None
    summary_dict['family_of_4'] = summary[0].text
    summary_dict['single'] = summary[1].text
    summary_dict['cost_comparison'] = summary[2].text
    summary_dict['rent_comparison'] = summary[3].text

    summary_df = pd.DataFrame([summary_dict])

    return summary_df

@task
def scrap_details(soup, country):
    logging = get_run_logger()
    logging.info('Scrapping details...')
    details = []    

    discretionary_values_table = soup.find('table', class_='data_wide_table new_bar_table')
    details_list = discretionary_values_table.find_all('tr')

    for element in details_list:
        detail_dict = {}
        if element.find('th'):
            continue
            
        valid_details = element.find_all('td')
        detail_dict['country'] = country
        detail_dict['city'] = None
        detail_dict['type'] = valid_details[0].text
        detail_dict['amount'] = valid_details[1].text
        detail_dict['range'] = valid_details[2].text
        
        details.append(detail_dict)

    details_df = pd.DataFrame(details)
    return details_df


@task
def scrap_numbeo(driver, engine):
    logging = get_run_logger()
    for country in COUNTRIES:
        logging.info(f'Executing: {country}')
        driver.get(MAIN_URL)

        try:
            accept_cookies = driver.find_element(By.ID,'accept-choices')
            accept_cookies.click()
            time.sleep(2)
        except Exception as e:
            logging.info('No cookies!')
           

        try:
            singn_up = driver.find_element(By.XPATH, "//button[@class='ui-button ui-widget ui-state-default ui-corner-all ui-button-icon-only ui-dialog-titlebar-close']")
            singn_up.click()  
            time.sleep(2)
        except Exception as e:
            logging.info('No Sign up!')
                       

        try:
            country_button = driver.find_element(By.XPATH, f"//a[contains(text(), '{country}')]")
            time.sleep(2)
            logging.info(f'Button to click: {country_button}')
            country_button.click()
            logging.info('After sleep on country click')
        except Exception as e:            
            logging.warning('Country not found!')
            logging.error(e)
            continue

        logging.info('Getting page source')
        page = driver.page_source
        soup = BeautifulSoup(page, 'html.parser')
        logging.debug('Source extracted')
        try:
            summary_df = scrap_summary(soup, country)
            details_df = scrap_details(soup, country)
        except Exception as e:
            print(e)        

        details_df.to_sql('country_expenses_detail', engine, schema='raw', if_exists='append', index=False)
        summary_df.to_sql('country_expenses', engine, schema='raw', if_exists='append', index=False)


@flow
def numbeo_scapper(retries=3, retry_delay_seconds=5, log_prints=True):
    logging = get_run_logger()
    driver = init_driver()
    engine = create_engine(CONNECTION_URL)
    state = scrap_numbeo(driver, engine, return_state=True)
    state.result(raise_on_failure=False)
    driver.quit()
    return True

    

if __name__ == '__main__':
    if ENV == 'PROD':
        numbeo_scapper.serve(
            name="scrapp-numbeo-living-cost",
            cron="00 22 * * 0-4",
            tags=["scrapper", "living-cost-analysis"],
            description="Scrap numbeo daily.",
            version="living-cost-analysis/deployment",
        )
    else:
        numbeo_scapper()    
