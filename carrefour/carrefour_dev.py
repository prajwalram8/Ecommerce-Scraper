import requests
import pandas as pd

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

params['currentPage'] = 0

url = url = f'https://www.carrefouruae.com/api/v8/categories/F1650300'

response = requests.get(url, headers=headers, params=params)

print(response.json())



# print(response_json)