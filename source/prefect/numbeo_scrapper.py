from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

COUNTRY = 'Portugal'

def accept_cookies(driver):
    accept_cookies = driver.find_element(By.ID,'accept-choices')
    accept_cookies.click()
    return

driver = webdriver.Chrome()
driver.get('https://www.numbeo.com/cost-of-living/')

for attempt in range(5):
    country = driver.find_element(By.XPATH, f"//a[contains(text(), '{COUNTRY}')]")

    try:
        country.click()
    except Exception as e:
        print(e)
        accept_cookies(driver)
        continue

    break

page = driver.page_source
soup = BeautifulSoup(page, 'html.parser')