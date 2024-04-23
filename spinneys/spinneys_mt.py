import os
import re
import json
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import setup_logging

logger = setup_logging('SPINNEYS')

BASE_URL = 'https://www.spinneys.com/en-ae/catalogue/'

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
    if 'impressions' in script_tag_contents:
        return True
    else:
        return 
    
def print_near_error(json_string, index):
    """
    Function to return the section of the parsed json that is throwing 
    error 
    """
    window = 100  # Adjust the window size to show more or less context
    start = max(0, index - window)
    end = min(len(json_string), index + window)
    return json_string[start:end]
    
def extract_info_from_grid(product):
    """
    Funtion to extract info from the product grid tag of the html
    """
    product_info = product.find('div', {'class': 'product-info'})
    product_name = product_info.find('p', {'class': 'product-name'}).find('a').text
    product_link = product_info.find('p', {'class': 'product-name'}).find('a')['href']
    product_price = product_info.find('p', {'class': 'product-price'}).find('span', {'class': 'price'}).text
    product_quantity = product_info.find('p', {'class': 'product-price'}).find('span', {'class': 'quantity'}).text.strip()
    return {
        'item_name': product_name, 
        'item_link': product_link, 
        'item_price': product_price, 
        'item_quantity': product_quantity
        }

def get_final_page_number():
    """
    Function to call the base url and extract the final page number
    """
    response = requests.get(url = BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
        
    page_num_tag = soup.find('div', {'class': 'page-numbers'}).find_all('a')
    page_num_tag = page_num_tag[-2].find('div', {'class': 'page-no-bx'}).text
    final_page_number = int(page_num_tag)

    return final_page_number

def process_page(page):
    
    params = {
            'page':page
        }

    items = []
    item_ids = []

    ## Step 1: Call URL with necessary headers
    response = requests.get(url = BASE_URL, headers=HEADERS, params=params )

    ## Step 2: Part the response into a soup object
    soup = BeautifulSoup(response.text, 'html.parser')

    ## Step 3.1: Extracting the Barcodes stores in the script tag
    script = soup.find_all('script')
    script_string = list(filter(lambda x: check_script(x), script))[0].text
    
    # Extract JSON-like part from the <script> tag
    pattern = r"dataLayer\.push\((\{.*?\})\);"
    match = re.search(pattern, script_string, re.DOTALL)

    if match:
        json_string = match.group(1)

        # Replace single quotes with double quotes
        json_string = json_string.replace("'", '"')

        # Remove potential trailing commas in lists or objects
        json_string = re.sub(r',\s*}', '}', json_string)
        json_string = re.sub(r',\s*\]', ']', json_string)

        # Parse JSON
        try:
            data = json.loads(json_string)
            item_ids = data['ecommerce']['impressions']
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e} for page: {page}")
            error_position = e.pos  # Using the exception's position attribute if available
            snippet = print_near_error(json_string, error_position)
            logger.error(f"Section of the JSON with issues \n {snippet}")
            return items
    else:
        logger.error(f"Could not find the JavaScript object in the script content for page {page}")

    ## Step 3.2: Extracting products from the grid
    grid = soup.find('div', {'class': 'arc-grid'}).find_all('div', {'class': 'js-product-wrapper product-bx'})
    grid_new = list(map(lambda x: extract_info_from_grid(x), grid))
    items.append(grid_new)

    ## Step 4: Joining the item ids with product info
    items = [{**x, **y} for x, y in zip(item_ids, items[0])]

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
        with open(os.path.join(local_stage,'spinneys.json'), 'w') as f:
            json.dump(all_items, f)
        return True
    else:
        return False
    
if __name__ == "__main__":
    pass
    # if main(local_stage='C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\spinneys'):
    #     print("Done!")