import os
import time
import json
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

options = webdriver.ChromeOptions()


class CarrefourCatExtractor:
    def __init__(self, category, stage_path):
        self.extraction_category = category
        self.local_stage_path = stage_path
        self.search_url = f'/api/v8/categories/{self.extraction_category}'
        self.call_url = f"https://www.carrefouruae.com/mafuae/en/c/{self.extraction_category}"
        self.driver = webdriver.Chrome(options=self.configure_browser_options())
        self.cookie_consent = False
        self.first_load_page = False

    def configure_browser_options(self):
        """Configure and return Chrome browser options."""
        options = webdriver.ChromeOptions()
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-extensions')
        options.add_argument('--no-sandbox')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        return options
    
    def first_load(self, page_source):
        page_source = BeautifulSoup(page_source,'html.parser')
        tag = page_source.find('script', {"type": "application/json"})
        json_data = json.loads(tag.string)
        products = json_data['props']['initialState']['search']['products']
        return products
    
    def get_search_responses(self, logs):
        """Extract response body from network logs for a specific target URL."""
        for log in logs:
            message = json.loads(log["message"])
            message_params = message.get("message", {}).get("params", {})

            response = message_params.get("response")
            if response and self.search_url in response.get("url", ""):
                request_id = message_params.get("requestId")
                if request_id:
                    try:
                        body = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': request_id})
                        return body
                    except Exception as e:
                        print(f"Error retrieving response body: {e}")
        return None

    def stage_json(self,json_obj, file_path):
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(json_obj, f, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"Exception {e} occured while staging the json object")
            return False
    
    def click_load_more(self):
        products = []
        wait = WebDriverWait(self.driver, 10)  # Timeout after 10 seconds

        if not self.cookie_consent:
            try:
                cookie_button = wait.until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                time.sleep(1)
                cookie_button.click()
                print("Cookie consent accepted.")
                self.cookie_consent = True
            except TimeoutException:
                print("No cookie consent button found or not clickable.")

        if not self.first_load_page:
            page_source = self.driver.page_source
            first_load_products = self.first_load(page_source)
            products.extend(first_load_products)
            self.first_load_page = True

        try: 
            # Ensure the button is in view and clickable
            load_more_button = wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "css-10s9ah"))
            )
            action = ActionChains(self.driver)
            action.move_to_element(load_more_button).perform()
            time.sleep(1)
            load_more_button.click()
            time.sleep(1)
        except TimeoutException:
            print("Load More Button Not found")
            return None 

        # Get Logs parse responses and save it as products to return
        logs = self.driver.get_log("performance")
        response_body = self.get_search_responses(logs=logs)

        if response_body and 'body' in response_body:
            response_body = json.loads(response_body['body'])
            products.extend(response_body.get('products', []))

        return products    
    
    def main(self):
        products = []
        try:
            self.driver.get(self.call_url)
            while True:
                output = self.click_load_more()
                if output is None:
                    break
                else:
                    products.extend(output)
        finally:
            print(f"Total products extracted: {len(products)}")
            if self.stage_json(
                json_obj = products, 
                file_path=os.path.join(
                    self.local_stage_path,
                    f'{self.extraction_category}_products.json'
                    )
                    ):
                print("Data successfully staged")
            else:
                print("Data stage issue")

            self.driver.close()
            self.driver.quit()

        return None


if __name__ == "__main__":
    category = 'F21630200'
    cat_extractor = CarrefourCatExtractor(
        category=category, 
        stage_path="C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\carrefour"
        )
    cat_extractor.main()

