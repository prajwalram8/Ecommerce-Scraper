import re
import os
import sys
import json
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.logger import setup_logging

logger = setup_logging('CHOITHRAMS')

BASE_URL = 'https://www.choithrams.com/en/catalogue/'

HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-AE,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

}

def check_script(script):
    """
    Function to check the list of script tags for the relevant script
    """
    script_tag_contents = script.text
    if 'view_item_list' in script_tag_contents:
        return True
    else:
        return False
    
def print_near_error(json_string, index):
    """
    Function to return the section of the parsed json that is throwing 
    error 
    """
    window = 100  # Adjust the window size to show more or less context
    start = max(0, index - window)
    end = min(len(json_string), index + window)
    return json_string[start:end]

def get_final_page_number():
    """
    Function to call the base url and extract the final page number
    """
    response = requests.get(url = BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
        
    page_num_tag = soup.find('div', {'class': 'page-buttons'}).find_all('a')
    page_num_tag = page_num_tag[-2].text
    final_page_number = int(page_num_tag)

    return final_page_number

def process_page(page):
    """
    Function to process extract data from a given page number input
    """

    params = {
        'page': page
    }

    items = []

    # Step 1: Call URL with necessary headers
    response = requests.get(url=BASE_URL, headers=HEADERS, params=params)

    # Step 2: Parse the response into a soup object
    soup = BeautifulSoup(response.text, 'html.parser')

    # Step 3.1: Extracting the items json file stored in the script tag
    script = soup.find_all('script')
    script_string = list(filter(lambda x: check_script(x), script))[0].text

    # Extract JSON-like part from the <script> tag
    pattern = r'"ecommerce": (\{.*?\})\s*\}'
    match = re.search(pattern, script_string, re.DOTALL)

    if match:
        json_string = match.group(1)

        # Replace JavaScript-style property names (without quotes) with quoted names
        json_string = re.sub(r'\s{2,}(\b\w+\b):\s', r'"\1":', json_string)

        # Decode HTML entities
        json_string = json_string.replace("&gt;", ">").replace("&amp;", "&")

        # Replace Python None with JSON null
        json_string = json_string.replace("None", "null")

        # Parse JSON
        try:
            data = json.loads(json_string)
            items = data['items']
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e} for page: {page}")
            error_position = e.pos  # Using the exception's position attribute if available
            snippet = print_near_error(json_string, error_position)
            logger.error(snippet)
            return items
    else:
        logger.error(f"Could not find the JavaScript object in the script content for page {page}")

    return items
    

def main(local_stage, num_workers=5):
    all_items = []
    final_page_number = get_final_page_number() 
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_page = {executor.submit(process_page, page): page for page in range(1, final_page_number + 1)}
            for future in tqdm(as_completed(future_to_page), total=len(future_to_page)):
                page_results = future.result()
                all_items.extend(page_results)

    if all_items:
        logger.info(f"Total Items Scraped {len(all_items)}")
        with open(os.path.join(local_stage,'choithrams.json'), 'w') as f:
            json.dump(all_items, f)
        return True
    else:
        return False
    
    
if __name__ == "__main__":
    pass

    # items= process_page_2(84)
    # print(len(items))
        
    # local_stage = "C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\choithrams"
    # if main(local_stage=local_stage, num_workers=7):
    #     print('Done!')