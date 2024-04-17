import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import json
from tqdm import tqdm


BASE_URL = 'https://www.spinneys.com/en-ae/catalogue/'

HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-AE,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

}

def check_script(script):
    script_tag_contents = script.text
    if 'impressions' in script_tag_contents:
        return True
    else:
        return False
    
def extract_info_from_grid(product):
    product_info = product.find('div', {'class': 'product-info'})
    product_name = product_info.find('p', {'class': 'product-name'}).find('a').text
    product_link = product_info.find('p', {'class': 'product-name'}).find('a')['href']
    product_price = product_info.find('p', {'class': 'product-price'}).find('span', {'class': 'price'}).text
    product_quantity = product_info.find('p', {'class': 'product-price'}).find('span', {'class': 'quantity'}).text.strip()
    return product_name, product_link, product_price, product_quantity

def get_final_page_number():
    response = requests.get(url = BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
        
    ## Step 3: Find Final Page Number and iterate across the pages
    page_num_tag = soup.find('div', {'class': 'page-numbers'}).find_all('a')
    page_num_tag = page_num_tag[-2].find('div', {'class': 'page-no-bx'}).text
    final_page_number = int(page_num_tag)

    return final_page_number


def main():
    all_items = []
    for page in tqdm(range(get_final_page_number() + 1)):

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
                impressions = data['ecommerce']['impressions']
                item_ids = [item['id'] for item in impressions]
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
        else:
            print("Could not find the JavaScript object in the script content.")

        ## Step 3.2: Extracting products from the grid
        grid = soup.find('div', {'class': 'arc-grid'}).find_all('div', {'class': 'js-product-wrapper product-bx'})
        grid_new = list(map(lambda x: extract_info_from_grid(x), grid))
        items.append(grid_new)

        ## Step 4: Joining the item ids with product info
        items = list(zip(item_ids, items[0]))
        items = [(t[0],) + t[1] for t in items]

        ## Step 5: Append into main item list
        all_items.append(items)
    
    all_items = all_items[0]

    return all_items


items = main()
items_df = pd.DataFrame(items, columns=['item_id', 'item_name', 'item_link', 'item_price', 'item_quantity'])
items_df.to_csv('Spinneys.csv')