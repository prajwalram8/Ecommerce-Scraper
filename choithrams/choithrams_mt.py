import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys


BASE_URL = 'https://www.choithrams.com/en/catalogue/'

HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-AE,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

}

def check_script(script):
    script_tag_contents = script.text
    if 'view_item_list' in script_tag_contents:
        return True
    else:
        return False


def get_final_page_number():
    response = requests.get(url = BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
        
    ## Step 3: Find Final Page Number and iterate across the pages
    page_num_tag = soup.find('div', {'class': 'page-buttons'}).find_all('a')
    page_num_tag = page_num_tag[-2].text
    final_page_number = int(page_num_tag)

    return final_page_number

def process_page(page):
    
    params = {
            'page':page
        }

    items = []

    ## Step 1: Call URL with necessary headers
    response = requests.get(url = BASE_URL, headers=HEADERS, params=params )

    ## Step 2: Part the response into a soup object
    soup = BeautifulSoup(response.text, 'html.parser')

    ## Step 3.1: Extracting the Barcodes stores in the script tag
    script = soup.find_all('script')
    script_string = list(filter(lambda x: check_script(x), script))[0].text

    # Extract JSON-like part from the <script> tag
    pattern = r'"ecommerce": (\{.*?\})\s*\}'
    match = re.search(pattern, script_string, re.DOTALL)

    if match:
        json_string = match.group(1)

        # Replace JavaScript-style property names (without quotes) with quoted names
        json_string = re.sub(r'(\w+):', r'"\1":', json_string)

        # Decode HTML entities
        json_string = json_string.replace("&gt;", ">").replace("&amp;", "&")

        # Handle any other specific clean-up your data needs that isn't covered by general rules
        # Replace Python None with JSON null
        json_string = json_string.replace("None", "null")

        
        # Parse JSON
        try:
            data = json.loads(json_string)
            items = data['items']
        except json.JSONDecodeError as e:
            with open('text.json', 'w') as f:
                json.dump(json_string, f)
            print(f"Error parsing JSON: {e} for page: {page}")
            
            error_position = 1313  # Example character position from your error message

            # Print the JSON snippet around the error
            snippet = print_near_error(json_string, error_position)
            print(snippet)
            sys.exit()
    else:
        print("Could not find the JavaScript object in the script content.")

    return items

def escape_quotes_in_json(json_string):
    # This function will attempt to find all JSON string values and escape internal quotes
    def escape_match(match):
        # Escape internal quotes and preserve the string enclosed by the outermost quotes
        escaped_content = match.group(0).replace('"', '\\"')
        # Make sure to revert the escaping for the first and last quote that define the string boundaries
        return '"' + escaped_content[1:-1] + '"'

    # Regex to match all string values, taking care not to match too greedily
    json_string = re.sub(r'(?<!\\)"(.*?)(?<!\\)"', escape_match, json_string, flags=re.DOTALL)
    return json_string

def process_page_2(page):

    params = {
        'page': page
    }

    items = []

    # Step 1: Call URL with necessary headers
    response = requests.get(url=BASE_URL, headers=HEADERS, params=params)

    # Step 2: Part the response into a soup object
    soup = BeautifulSoup(response.text, 'html.parser')

    # Step 3.1: Extracting the Barcodes stored in the script tag
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

        # Escape quotes within JSON strings
        # json_string = escape_quotes_in_json(json_string)

        # Replace Python None with JSON null
        json_string = json_string.replace("None", "null")

        # Parse JSON
        try:
            data = json.loads(json_string)
            items = data['items']
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e} for page: {page}")
            error_position = e.pos  # Using the exception's position attribute if available
            snippet = print_near_error(json_string, error_position)
            print(snippet)
            sys.exit()
    else:
        print("Could not find the JavaScript object in the script content.")


    return items
    

def main(local_stage, num_workers=5):
    all_items = []
    final_page_number = get_final_page_number() 
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_page = {executor.submit(process_page_2, page): page for page in range(1, final_page_number + 1)}
            for future in tqdm(as_completed(future_to_page), total=len(future_to_page)):
                page_results = future.result()
                all_items.extend(page_results)

    if all_items:
        print(len(all_items))
        with open(os.path.join(local_stage,'choithrams.json'), 'w') as f:
            json.dump(all_items, f)
        return True
    else:
        return False
    

def print_near_error(json_string, index):
    window = 100  # Adjust the window size to show more or less context
    start = max(0, index - window)
    end = min(len(json_string), index + window)
    return json_string[start:end]

    
if __name__ == "__main__":
    # items= process_page_2(84)
    # print(len(items))

        
    local_stage = "C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\choithrams"
    if main(local_stage=local_stage, num_workers=7):
        print('Done!')