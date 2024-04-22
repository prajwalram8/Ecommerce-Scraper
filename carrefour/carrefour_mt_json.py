import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm 
import os
import json
from utils.logger import setup_logging

logger = setup_logging('Carrefour-MT-JSON')

category_master = pd.read_csv('menu.csv')

categories = [
    'F1600000','F11600000','F1700000','F1500000','F6000000',
    'F1610000','F1200000','NF3000000','NF2000000','F1000000',
    'NF8000000','F1100000','NF7000000'
    ]

subcategories = category_master[
    category_master['L1_ID'].isin(categories)
    ]['L3_ID'].to_list()

params = {
    'filter': '',
    'sortBy': 'relevance',
    'currentPage': 0,
    'pageSize': 60,
    'maxPrice': '',
    'minPrice': '',
    'areaCode': 'DubaiFestivalCity-Dubai',
    'lang': 'en',
    'displayCurr': 'AED',
    'latitude': '25.2107038',
    'longitude': '55.2755447',
    'needVariantsData': 'true',
    'nextOffset': '',
    'requireSponsProducts': 'true',
    'responseWithCatTree': 'true',
    'depth': '3'
}

# Headers
headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-AE,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'Appid': 'Reactweb',
    'Credentials': 'include',
    # 'Deviceid': '1298535093.1704889303',
    'Env': 'prod',
    'If-Modified-Since': 'Wed, 10 Jan 2024 12:21:55 GMT',
    'Intent': 'STANDARD',
    'Referer': 'https://www.carrefouruae.com',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Storeid': 'mafuae',
    # 'Token': 'undefined',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # 'Userid': '2127C657-51AB-0352-DE99-EA65C2F3CC1D'
}


def fetch_category_data(cat):
    url = f'https://www.carrefouruae.com/api/v8/categories/{cat}'
    params['currentPage'] = 0
    response = requests.get(url, headers=headers, params=params)
    df_list = []
    try:
        response_json = response.json()
        if 'products' in response_json:
            df_list.append(response_json['products'])
        for page in range(1, int(response_json.get('numOfPages', 1))):
            params['currentPage'] = page
            response = requests.get(url, headers=headers, params=params)
            response_json = response.json()
            df_list.append(response_json['products'])
        return df_list if df_list else []
    except Exception as e:
        print(f"Error processing category {cat}: {e}")
        return []

def main(local_stage, num_workers=5):
    results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_cat = {executor.submit(fetch_category_data, cat): cat for cat in subcategories}
        for future in tqdm(as_completed(future_to_cat), total=len(future_to_cat)):
            cat_results = future.result()
            results.extend(cat_results)  # Append DataFrame to results list

    if results:  # Check if the list is not empty
        ls = [subeach for each in results for subeach in each]
        print(len(ls))
        with open(os.path.join(local_stage,'carrefour.json'), 'w') as f:
            json.dump(ls, f)
        return True
    else:
        return False

if __name__ == "__main__":
    main(local_stage='./data', num_workers=5)
