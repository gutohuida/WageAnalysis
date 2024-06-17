# Standard library imports
import os
import time
import json
import random
from datetime import datetime

# Third-party library imports
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
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
EXEC = os.environ.get('EXEC')


REGION_DICT = {
                'EU': EU_COUNTRIES,
                'NA': NA_COUNTRIES + OCE_COUNTRIES,
                'SA': SA_COUNTRIES,
                'AS': ASIAN_COUNTRIES,
                'AF': AF_COUNTRIES
            }
EXECUTION_ORDER = {
                '6': 'EU',
                '0': 'NA',
                '1': 'SA',
                '2': 'AS',
                '3': 'AF'
            }           

DAY_OF_WEEK = datetime.today().weekday()

COUNTRIES = []
if EXEC == 'manual':
    REGION = json.loads(os.environ.get('MANUAL_EXEC'))
    for region in REGION:
        COUNTRIES = COUNTRIES + REGION_DICT[region]
else:
    REGION = EXECUTION_ORDER[DAY_OF_WEEK]
    COUNTRIES = REGION_DICT[REGION]

MAIN_URL = os.environ.get('MAIN_URL')

PAGE_TIME_OUT = os.environ.get('PAGE_TIME_OUT')

PROXY_COUNTRIES = [
    "United States", "Canada"
]

DB_USER = os.environ.get('DB_USER')
DB_PSSWRD = os.environ.get('DB_PSSWRD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

CONNECTION_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PSSWRD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

JOBS = [
    "Software Engineer", "Senior Software Engineer", "Systems Analyst", "Senior Web Developer","Web Developer", 
    "Network Engineer", "Database Administrator", "Senior Database Administrator", "Data Scientist", "Senior Data Scientist", "Security Analyst",
    "Cloud Engineer", "DevOps Engineer", "Mobile Developer", "Senior Mobile Developer",
    "Machine Learning Engineer", "UX/UI Designer", "Quality Engineer", "Project Manager"
    "IT Project Manager", "Senior Project Manager", "Technical Support Specialist", "IT Consultant",
    "Game Developer", "Computer Vision Engineer", "AI Research Scientist",
    "Data Engineer", "Senior Data Engineer", "Data Analyst", "Senior Data Analyst", "Full Stack Developer"
    ]

REMOVED_JOBS = ["Blockchain Developer", "IoT Developer", "Ethical Hacker"]
 

if ENV == 'DEV':
    REGION = 'EU'
    JOBS = ["Data Engineer"]
    COUNTRIES = ["Portugal"]                      

## TASKS ##
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
        if all_data[6].text == 'yes' and (all_data[3].text in PROXY_COUNTRIES):
            url = all_data[0].text
            port = all_data[1].text
            location = all_data[3].text
            proxy_list.append((f'{url}:{port}',location))

    logging.info(f'Proxy_List: {proxy_list}')
    return proxy_list

@task
def init_driver(proxy_list=None, driver=None):
    logging = get_run_logger()
    if driver:
        driver.quit()  
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

@task
def navigate_country_job(driver: webdriver, job: str, country: str):
    input_element_job = driver.find_element(By.NAME, 'typedKeyword')
    input_element_job.send_keys(job)
    input_element_location = driver.find_element(By.NAME, 'Autocomplete')
    input_element_location.send_keys(country)
    driver.implicitly_wait(10)
    location = driver.find_element(By.XPATH, "(//div[@data-test='location-autocompleteContainer']/ul/li/div/span/span/span)[1]")
    location.click()
    time.sleep(1)
    input_element_location.submit()

@task
def scrap_country_info(soup, country, job):
    country_stats = {}
    aditional_pay = soup.find('div', {'data-test':'additional-pay-breakdown-only-one'}).find('span') if soup.find('div', {'data-test':'additional-pay-breakdown-only-one'}) else None

    country_stats = {
        'country': country,
        'job': job,
        'estimated_pay': soup.find('h3',{'class':'m-0 css-16zrpia el6ke054'}).text,
        'period':soup.find('span',{'class':'m-0 css-1in2cw4 el6ke050'}).text,
        'last_update': soup.find('span',{'class':'css-1in2cw4 el6ke050'}).text,
        'wage_text': soup.find('p',{'class':'css-79elbk m-0 css-1in2cw4 el6ke053'}).find('span').text,
        'pay_range': soup.find('span',class_='css-uakwcr ebx6x3o1').text,
        'base_pay': soup.find('div', {'data-test':'base-pay'}).find('span').text,
        'additional_pay': aditional_pay.text if aditional_pay else '0'

    }
    return country_stats

@task
def scrap_jobs(popular_jobs, country, job, wage_text):
    logging = get_run_logger()
    jobs_list = []
    for popular_job in popular_jobs:
        jobs_anex = {}
        try:            
            open_jobs_aux = popular_job.find('div', class_='col-12 col-md-3 col-lg-3 d-none d-lg-block align-self-start')  
            min_aux = popular_job.find('div', class_='d-flex flex-column align-items-end css-15o6gsn e13r6qcv5')
            max_aux = popular_job.find('div', class_='d-flex flex-column align-items-end css-1qfy6mj e13r6qcv5')
            period_aux = popular_job.find('div', class_='mb-xxsm d-flex align-items-baseline css-3tpw1n e13r6qcv1')

            company = popular_job.find('a', class_='css-1ikln7a el6ke052').text
            score = popular_job.find('span', class_='css-h9sogr m-0 css-60s9ld el6ke050')
            open_jobs = open_jobs_aux.find('span') if open_jobs_aux else None                          
            data_collected =  popular_job.find('a', class_='m-0 d-flex css-259095 e1aj7ssy6')
            min = min_aux.find('span', class_='m-0 css-1in2cw4 el6ke050') if min_aux else None
            max = max_aux.find('span', class_='m-0 css-1in2cw4 el6ke050') if max_aux else None
            likely = popular_job.find('h3', class_='m-0 css-16zrpia el6ke054')
            period = period_aux.find('span', class_='m-0 css-1in2cw4 el6ke050') if period_aux else None

            jobs_anex = {
                'country': country,
                'job': job,
                'wage_text': wage_text,
                'company': company,
                'score': score.text if score else None,
                'open_jobs': open_jobs.text if open_jobs else None,
                'data_collected': data_collected.text if data_collected else None,
                'min': min.text if min else None,
                'max': max.text if max else None,
                'likely': likely.text if likely else None,
                'period': period.text if period else None
            }
        except Exception as e:
            logging.error(f'scrap_glassdoor: {job} - {country} - {company} Msg: {e}')

        if jobs_anex:
            jobs_list.append(jobs_anex)

    jobs_df = pd.DataFrame(jobs_list) 

    return jobs_df

@task
def scrap_glassdoor(driver, proxy_list, engine):
    logging = get_run_logger()
    for job in JOBS:
        for country in COUNTRIES:
            country_stat_pd = pd.DataFrame()            
            country_stats = {}

            for attempt in range(10):
                try:
                    driver.get(MAIN_URL)
                    navigate_country_job(driver, job, country)
                    page = driver.page_source
                    soup = BeautifulSoup(page, 'html.parser')
                except Exception as e:
                    logging.error(f'scrap_glassdoor: {e}')
                    if attempt == 5:                        
                        driver = init_driver(driver=driver)
                        proxy_list = get_proxy_list(driver)
                    elif attempt == 8:
                        proxy_list = None   

                    logging.info("Closing driver...")                    
                    driver = init_driver(proxy_list, driver)
                    continue

                break

            try:
                country_stats = scrap_country_info(soup, country, job)
                wage_text = soup.find('p',{'class':'css-79elbk m-0 css-1in2cw4 el6ke053'}).find('span').text
                
            except Exception as e:
                logging.error(f'Error on {job} in {country}: {e}')    
            
            try:               
                if soup.find('h1', class_='m-0 css-zb88ad el6ke056'):
                    if soup.find('h1', class_='m-0 css-zb88ad el6ke056').text == 'Please try a new search':
                        logging.info(f'No results found for {job} in {country}')
                        continue                  
            except Exception as e:    
                logging.error(f'{e}')
                
            country_stat_pd = pd.concat([country_stat_pd, pd.DataFrame(country_stats, index=[0])], ignore_index=True)

            for attempt in range(5):
                try:
                    jobs_block  = soup.find('div', class_='col-lg px-0')
                    popular_jobs = jobs_block.find_all('div', class_='py d-flex align-items-start align-items-lg-center css-17435dd row')
                except Exception as e:
                    logging.error(f'No jobs block found for {job} in {country} - {e}')
                    continue    
                        
                jobs_df = scrap_jobs(popular_jobs, country, job, wage_text)                                 
            
                break            

            time.sleep(1)

            if ENV == 'DEV':
                logging.info(country_stat_pd.head())
                logging.info(jobs_df.head())
                return True

            try:
                country_stat_pd.to_sql('country_job_info', engine, schema='raw', if_exists='append', index=False)
                jobs_df.to_sql('popular_companies', engine, schema='raw', if_exists='append', index=False)
            except Exception as e:
                logging.error(f'{job} - {country} - Failed to load data: {e}') 

## FLOWS ##
@flow
def glassdoor_scrapper(retries=3, retry_delay_seconds=5, log_prints=True):
    logging = get_run_logger()
    logging.info('Starting glassdoor scrapping')
    logging.info(f'Running for region: {REGION} - countries: {COUNTRIES} - jobs: {JOBS} - env: {ENV}')
    driver = init_driver()
    proxy_list = get_proxy_list(driver)
    driver = init_driver(proxy_list, driver)
    engine = creat_db_connection()
    state = scrap_glassdoor(driver, proxy_list, engine, return_state=True)
    state.result(raise_on_failure=False)
    driver.quit()
    return True


if __name__ == "__main__":
    glassdoor_scrapper.serve(
        name="scrapp-glassdoor-salarys",
        cron="45 22 * * 0-4",
        tags=["scrapper", "wage-analysis"],
        description="Scrap glassdoor daily.",
        version="wage-analysis/deployment",
    )
    
