import requests
import pandas as pd
import json

category_master = pd.read_csv('menu2.csv')

categories = ['F1600000','F11600000','F1700000','F1500000','F6000000','F1610000','F1200000','NF3000000','NF2000000','F1000000','NF8000000','F1100000','NF7000000']

subcategories = category_master[category_master['L1_ID'].isin(categories)]['L3_ID'].to_list()

# URL and Query Parameters

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
    'Deviceid': '1298535093.1704889303',
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
    'Token': 'undefined',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Userid': '2127C657-51AB-0352-DE99-EA65C2F3CC1D'
}
df_list = []

for cat in subcategories:
    print(cat)
    counter = 0
    params['currentPage'] = 0

    url = url = f'https://www.carrefouruae.com/api/v8/categories/{cat}'

    response = requests.get(url, headers=headers, params=params)

    try:
        response_json = response.json()
        # print(response_json['numOfPages'], response_json['totalProducts'])
    except KeyError:
        print(response_json)
        print(response.url)
        pass

    df_list.append(pd.json_normalize(response_json['products']))
    counter+=len(response_json['products'])
    
    try:
        for page in range(1, int(response_json['numOfPages'])):
            params['currentPage'] = page+1
            response = requests.get(url, headers=headers, params=params)
            response_json = response.json()
            df_list.append(pd.json_normalize(response_json['products']))
            counter+=len(response_json['products'])
            # print(response.url)
        print(f"For Category {cat}: {counter}")
    except KeyError:
        print(response_json)
        print(response.url)
        pass


final_df = pd.concat(df_list,axis=0)

final_df.to_csv('final_df2.csv')



