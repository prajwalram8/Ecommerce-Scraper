from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time

from bs4 import BeautifulSoup

import json

# Set up Chrome options
options = Options()
# options.add_argument("--headless")  # Run headless
options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration
options.add_argument("--start-fullscreen")
# options.add_argument("--window-size=1920x1080")  # Set the window size

# # Enabling performance logging
# options.set_capability("goog:loggingPrefs", {'performance': 'ALL'})
# options.add_experimental_option("perfLoggingPrefs", {"enableNetwork": True, "enablePage": True})
# options.add_argument('--ignore-certificate-errors')

# Specify the path to ChromeDriver
path_to_chromedriver = 'chromedriver.exe'  # Change this to your actual path
service = Service(executable_path=path_to_chromedriver)

# Initialize WebDriver
driver = webdriver.Chrome(service=service, options=options)

# Navigate to the website
driver.get("https://www.carrefouruae.com/mafuae/en/c/F21630200")


def identify_and_click(click=True):
    # Wait until the button is present in the DOM and is visible
    button = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "button[data-testid='trolly-button']"))
    )
    # Scroll the button into view and then click on it
    driver.execute_script("arguments[0].scrollIntoView(true);", button)
    
    if click:
        button.click()

    return True


import time
all_products = []

try:
    if identify_and_click():
        time.sleep(5)
        print(driver.current_url)
        page_source = driver.page_source
        page_source = BeautifulSoup(page_source,'html.parser')
        tag = page_source.find('script', {"type": "application/json"})
        json_data = json.loads(tag.string)
        products = json_data['props']['initialState']['search']['products']
        all_products.extend(products)
    
    if identify_and_click():
        time.sleep(5)
        print(driver.current_url)
        driver.refresh()
        page_source = driver.page_source
        page_source = BeautifulSoup(page_source,'html.parser')
        tag = page_source.find('script', {"type": "application/json"})
        json_data = json.loads(tag.string)
        products = json_data['props']['initialState']['search']['products']
        all_products.extend(products)
    
    print(identify_and_click(False))
    print(driver.current_url)
    
    print(len(all_products))
    with open('carrefour_sel.json', 'w') as f:
        json.dump(all_products, f)
        
except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    driver.quit()
