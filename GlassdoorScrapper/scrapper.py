from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

TARGET_URL = "https://www.glassdoor.com/Salaries/portugal-data-engineer-salary-SRCH_IL.0,8_IN195_KO9,22.htm"
MAIN_URL = "https://www.glassdoor.com/Salaries/index.htm"
JOBS = ['Data Engineer']
COUNTRYS = ['Portugal', 'Brazil', 'France', 'Germany', 'Spain']

def test_driver(target_url):
    driver=webdriver.Chrome()
    driver.get(target_url)
    driver.maximize_window()
    time.sleep(2)
    return

#test_driver(TARGET_URL)

driver=webdriver.Chrome()

for job in JOBS:
    for country in COUNTRYS:

        driver.get(MAIN_URL)

        input_element_job = driver.find_element(By.NAME, "typedKeyword")
        input_element_job.send_keys(job)
        input_element_location = driver.find_element(By.NAME, "Autocomplete")
        input_element_location.send_keys(country)
        driver.implicitly_wait(10)
        location = driver.find_element(By.XPATH, "(//div[@data-test='location-autocompleteContainer']/ul/li/div/span/span/span)[1]")
        location.click()
        input_element_location.submit()

        page = driver.page_source
        soup = BeautifulSoup(page, 'html.parser')

        medium_wage = soup.find("h3",{"class":"m-0 css-16zrpia el6ke054"}).text
        print(f'{job} in {country}: {medium_wage}')
        time.sleep(3)


