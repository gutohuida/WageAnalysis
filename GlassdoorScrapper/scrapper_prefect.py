import time 
import random
import logging
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from prefect import flow, task, get_run_logger

PAGE_TIME_OUT = 15

PROXY_COUNTRYS = [
    "United States", "Canada"
]

CONNECTION_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/wageanalysis"

@task
def creat_db_connection():
    postgresql_url = CONNECTION_URL
    engine = create_engine(postgresql_url)
    
    return engine

@task
def get_proxy_list(driver):
    logging = get_run_logger()
    logging.info('scrapping poxys')
    for attempt in range(5):
        try:
            driver.get('https://free-proxy-list.net/')
            page = driver.page_source
            soup = BeautifulSoup(page, 'html.parser')

            proxy_table = soup.find('table', class_='table table-striped table-bordered').find('tbody')
        except Exception as e:
            logging.error(f'GET PROXY LIST: {e}')
            time.sleep(2)
            continue
        break

    proxy_list = []
    for proxy_line in proxy_table.find_all('tr'):
        all_data = proxy_line.find_all('td')
        logging.debug(f'Proxy list lines: {all_data}')
        if all_data[6].text == 'yes' and (all_data[3].text in PROXY_COUNTRYS):
            url = all_data[0].text
            port = all_data[1].text
            location = all_data[3].text
            proxy_list.append((f'{url}:{port}',location))

    logging.info(f'Proxy_List: {proxy_list}')
    return proxy_list

@task
def init_driver(proxy_list=None):
    logging = get_run_logger()
      
    if proxy_list:
        chrome_options = webdriver.ChromeOptions() 
        random_proxy = random.choice(proxy_list)
        logging.info(f'Proxy: {random_proxy}') 
        chrome_options.add_argument(f'--proxy-server={random_proxy[0]}') 
        driver = webdriver.Chrome(options=chrome_options)
        
    else:
        driver = webdriver.Chrome()          

    driver.set_page_load_timeout(PAGE_TIME_OUT)
    return driver

@flow
def glassdoor_scrapper(retries=3, retry_delay_seconds=5, log_prints=True):
    logging = get_run_logger()
    logging.info('Starting glassdoor scrapping')
    driver = init_driver()
    proxy_list = get_proxy_list(driver)
    driver.close()
    driver = init_driver(proxy_list)
    engine = creat_db_connection()
    


if __name__ == "__main__":
    glassdoor_scrapper()