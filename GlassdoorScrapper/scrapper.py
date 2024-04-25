import time
import json

#import pandas as pd
#import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By


MAIN_URL = 'https://www.glassdoor.com/Salaries/index.htm'
JOBS = ['Data Engineer']
COUNTRYS = ['Portugal']

def test_driver(target_url):
    driver=webdriver.Chrome()
    driver.get(target_url)
    driver.maximize_window()
    time.sleep(2)
    return

def job_country_page(driver: webdriver, job: str, country: str):
    input_element_job = driver.find_element(By.NAME, 'typedKeyword')
    input_element_job.send_keys(job)
    input_element_location = driver.find_element(By.NAME, 'Autocomplete')
    input_element_location.send_keys(country)
    driver.implicitly_wait(10)
    location = driver.find_element(By.XPATH, "(//div[@data-test='location-autocompleteContainer']/ul/li/div/span/span/span)[1]")
    location.click()
    time.sleep(3)
    input_element_location.submit()


#test_driver(TARGET_URL)

driver=webdriver.Chrome()

#driver = uc.Chrome(headless=False,use_subprocess=True)

for job in JOBS:
    for country in COUNTRYS:

        driver=webdriver.Chrome()

        driver.get(MAIN_URL)

        job_country_page(driver, job, country)

        page = driver.page_source
        soup = BeautifulSoup(page, 'html.parser')

        a = {
            'estimated_pay': soup.find('h3',{'class':'m-0 css-16zrpia el6ke054'}).text,
            'period':soup.find('span',{'class':'m-0 css-1in2cw4 el6ke050'}).text,
            'update_date': soup.find('span',{'class':'css-1in2cw4 el6ke050'}).text,
            'wage_text': soup.find('p',{'class':'css-79elbk m-0 css-1in2cw4 el6ke053'}).find('span').text,
            'pay_range': soup.find('span',class_='css-uakwcr ebx6x3o1').text,
            'base_pay': soup.find('div', {'data-test':'base-pay'}).find('span').text,
            'additional_pay': soup.find('div', {'data-test':'additional-pay-breakdown-only-one'}).find('span').text

        }
        
        jobs_block  = soup.find('div', class_='col-lg px-0')
        all_jobs = jobs_block.find_all('div', class_='py d-flex align-items-start align-items-lg-center css-17435dd row')
        jobs = {}
        for job in all_jobs:
            #print(job.find('div', class_='col-12 col-md-3 col-lg-3 d-none d-lg-block align-self-start').find('span').text)
            open_jobs = job.find('div', class_='col-12 col-md-3 col-lg-3 d-none d-lg-block align-self-start').find('span')
            jobs[job.find('a', class_='css-1ikln7a el6ke052').text] = {
                'score': job.find('span', class_='css-h9sogr m-0 css-60s9ld el6ke050').text,
                'open_jobs': open_jobs.text if open_jobs else '0',
                'data_collected': job.find('a', class_='m-0 d-flex css-259095 e1aj7ssy6').text,
                'min': job.find('div', class_='d-flex flex-column align-items-end css-15o6gsn e13r6qcv5').find('span', class_='m-0 css-1in2cw4 el6ke050').text,
                'max': job.find('div', class_='d-flex flex-column align-items-end css-1qfy6mj e13r6qcv5').find('span', class_='m-0 css-1in2cw4 el6ke050').text,
                'likely':job.find('h3', class_='m-0 css-16zrpia el6ke054').text,
                'period': job.find('div', class_='mb-xxsm d-flex align-items-baseline css-3tpw1n e13r6qcv1').find('span', class_='m-0 css-1in2cw4 el6ke050').text
            }

            print(f"Job: {job.find('a', class_='css-1ikln7a el6ke052').text}\n{json.dumps(jobs[job.find('a', class_='css-1ikln7a el6ke052').text], indent=4)}\n")

        print(f"{[key + ': ' + a[key] for key in a]}")
        time.sleep(3)



