import time
import os  
import random
import logging
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By


MAIN_URL = 'https://www.glassdoor.com/Salaries/index.htm'

JOBS = [
    "Software Engineer", "Senior Software Engineer", "Systems Analyst", "Senior Web Developer","Web Developer", 
    "Network Engineer", "Database Administrator", "Senior Database Administrator", "Data Scientist", "Security Analyst",
    "Cloud Engineer", "DevOps Engineer", "Mobile Developer", "Senior Mobile Developer",
    "Machine Learning Engineer", "UX/UI Designer", "Quality Engineer",
    "Project Manager", "Senior Project Manager", "Technical Support Specialist", "IT Consultant",
    "Game Developer", "Computer Vision Engineer", "AI Research Scientist",
    "Blockchain Developer", "IoT Developer", "Ethical Hacker", "Data Engineer", "Senior Data Engineer",
    "Data Analyst", "Senior Data Analyst", "Full Stack Developer"
 ]
#"Mobile Developer", "Quality Engineer", "Project Manager", Cloud Solutions Architect

COUNTRYS = [
    "Germany", "United Kingdom", "France", "Italy", "Russia", "Spain",
    "Netherlands", "Switzerland", "Turkey", "Sweden", "Poland", "Belgium",
    "Austria", "Ireland", "Norway", "Denmark", "Finland", "Portugal",
    "Czech Republic", "Greece", "Romania", "Hungary", "Slovakia", "Luxembourg",
    "Bulgaria", "Croatia", "Lithuania", "Slovenia", "Latvia", "Estonia",
    "Cyprus", "Malta", "Iceland", "Bosnia and Herzegovina", "Albania",
    "North Macedonia", "Moldova", "Montenegro", "Serbia", "Belarus",
    "Ukraine", "United States", "Canada", "Mexico", "Brazil", "Argentina", "Colombia",
    "China", "India", "Japan", "South Korea", "Saudi Arabia", "Iran", "Israel", "Turkey",
    "Nigeria", "South Africa", "Egypt", "Ethiopia", "Australia", "New Zealand"
]

PROXY_COUNTRYS = [
    "United States", "Canada"
]

PAGE_TIME_OUT = 15

def test_driver(target_url):
    driver=webdriver.Chrome()
    driver.get(target_url)
    driver.maximize_window()
    time.sleep(2)
    return

def test_proxy(driver):    
    try:
        driver.get("http://httpbin.org/ip")
        origin = driver.find_element(By.TAG_NAME, "body").text
        if not 'origin' in origin:
            driver.close()
            return False
        return True
    except Exception as e:
        return False    

def get_proxy_list(driver):
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

    logging.debug(f'Proxy_List: {proxy_list}')
    return proxy_list    

def job_country_page(driver: webdriver, job: str, country: str):
    input_element_job = driver.find_element(By.NAME, 'typedKeyword')
    input_element_job.send_keys(job)
    input_element_location = driver.find_element(By.NAME, 'Autocomplete')
    input_element_location.send_keys(country)
    driver.implicitly_wait(10)
    location = driver.find_element(By.XPATH, "(//div[@data-test='location-autocompleteContainer']/ul/li/div/span/span/span)[1]")
    location.click()
    time.sleep(1)
    input_element_location.submit()

def init_driver(proxy_list=None):  
      
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

def creat_db_connection():
    postgresql_url = 'postgresql+psycopg2://postgres:postgres@localhost:5432/wageanalysis'
    engine = create_engine(postgresql_url)
    
    return engine

def scrap_glassdoor(driver, proxy_list, engine):
    for job in JOBS:
        for country in COUNTRYS:
            country_stat_pd = pd.DataFrame()
            jobs_df = pd.DataFrame()

            for attempt in range(10):
                try:
                    driver.get(MAIN_URL)
                    job_country_page(driver, job, country)
                    page = driver.page_source
                    soup = BeautifulSoup(page, 'html.parser')
                except Exception as e:
                    logging.error(f'scrap_glassdoor: {e}')
                    if attempt == 5:
                        driver.close()
                        driver = init_driver()
                        proxy_list = get_proxy_list(driver)
                    elif attempt == 8:
                        proxy_list = None   

                    driver.close()
                    driver = init_driver(proxy_list)
                    continue

                break

            try:
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
                        
                for popular_job in popular_jobs:
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
                            'company': company,
                            'score': score.text if score else None,
                            'open_jobs': open_jobs.text if open_jobs else None,
                            'data_collected': data_collected.text if data_collected else None,
                            'min': min.text if min else None,
                            'max': max.text if max else None,
                            'likely': likely.text if likely else None,
                            'period': period.text if period else None
                        }

                        jobs_df = pd.concat([jobs_df, pd.DataFrame(jobs_anex, index=[0])], ignore_index=True)
                    except Exception as e:
                        logging.error(f'scrap_glassdoor: {job} - {country} - {company} Msg: {e}')
            
                break            

            time.sleep(1)
            #os.makedirs(f'data/{job}/{country}', exist_ok=True)  
            try:
                country_stat_pd.to_sql('country_job_info', engine, schema='raw', if_exists='append', index=False)
                jobs_df.to_sql('popular_companies', engine, schema='raw', if_exists='append', index=False)
            except Exception as e:
                logging.error(f'{job} - {country} - Failed to load data: {e}')    
            #country_stat_pd.to_csv(f'data/{job}/{country}/general_stats.csv')
            #jobs_df.to_csv(f'data/{job}/{country}/companies.csv')    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('Starting glassdoor scrapping')
    driver = init_driver()
    proxy_list = get_proxy_list(driver)
    driver.close()
    driver = init_driver(proxy_list)
    engine = creat_db_connection()
    scrap_glassdoor(driver, proxy_list, engine)
    logging.info('Glassdoor scrapping finished')
    



