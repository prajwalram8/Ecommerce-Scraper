import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        options.add_argument('--headless')  # Runs Chrome in headless mode.
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-extensions')
        options.add_argument('--no-sandbox')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        return options

    def first_load(self, page_source):
        """Extract initial product list from the first page load."""
        soup = BeautifulSoup(page_source, 'html.parser')
        tag = soup.find('script', {"type": "application/json"})
        if tag:
            json_data = json.loads(tag.string)
            return json_data['props']['initialState']['search']['products']
        return []

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
                        logging.error(f"Error retrieving response body: {e}")
        return None

    def stage_json(self, json_obj, file_path):
        """Save JSON object to file."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(json_obj, f, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            logging.error(f"Exception occurred while staging the JSON object: {e}")
            return False
    
    def click_load_more(self):
        """Click the 'Load More' button to load additional products."""
        products = []
        wait = WebDriverWait(self.driver, 10)
        if not self.cookie_consent:
            try:
                cookie_button = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                cookie_button.click()
                logging.info("Cookie consent accepted.")
                self.cookie_consent = True
            except TimeoutException:
                logging.warning("No cookie consent button found or not clickable.")

        if not self.first_load_page:
            page_source = self.driver.page_source
            first_load_products = self.first_load(page_source)
            products.extend(first_load_products)
            self.first_load_page = True

        try:
            load_more_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "css-10s9ah")))
            ActionChains(self.driver).move_to_element(load_more_button).click().perform()
        except TimeoutException:
            logging.warning("Load More Button not found")
            return None 

        logs = self.driver.get_log("performance")
        response_body = self.get_search_responses(logs)

        if response_body and 'body' in response_body:
            response_body = json.loads(response_body['body'])
            products.extend(response_body.get('products', []))

        return products    
    
    def main(self):
        """Main method to orchestrate the extraction process."""
        products = []
        self.driver.get(self.call_url)
        while True:
            output = self.click_load_more()
            if output is None:
                break
            products.extend(output)

        logging.info(f"Total products extracted: {len(products)}")
        if self.stage_json(products, os.path.join(self.local_stage_path, f'{self.extraction_category}_products.json')):
            logging.info("Data successfully staged")
        else:
            logging.warning("Data staging issue")
        self.driver.quit()

def scrape_category(category, stage_path):
    """Function to initiate scraping for a specific category."""
    extractor = CarrefourCatExtractor(category=category, stage_path=stage_path)
    extractor.main()

def run_parallel_extraction(categories, stage_path):
    """Run the extraction in parallel across multiple categories."""
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_category, cat, stage_path) for cat in categories]
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions caught during the thread execution.

if __name__ == "__main__":
    category = 'F21630200'
    stage_path = "C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\carrefour"
    scrape_category(category=category, stage_path=stage_path)

    # categories = ['F21630200', 'F21630201', 'F21630202']  # Example categories
    # stage_path = "path_to_data_directory"
    # run_parallel_extraction(categories, stage_path)
