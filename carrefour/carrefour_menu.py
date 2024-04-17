
import requests
import pandas as pd
import json


URL = 'https://www.carrefouruae.com/api/v1/menu'

params = {
    'latitude': '25.2321031',
    'longitude': '55.2772914',
    'lang': 'en',
    'displayCurr':'AED'
}

headers = {
    'Appid': 'Reactweb',
    'Env': 'prod',
    'Lang': 'en',
    'Langcode': 'en',
    'Referer': 'https://www.carrefouruae.com',
    'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Storeid': 'mafuae',
    'Token': 'undefined',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Userid': 'anonymous',
    'X-Requested-With': 'XMLHttpRequest'
}

# Make the GET request
response = requests.get(URL, headers=headers, params=params)

# category_master =

# for each in response.json()[0]['children']:
#     print(each['id'], each['name'])
#     for i in each['children']:
#         print('----',i['id'], i['name'])
#         for j in i['children']:
#             print('---- ----',j['id'], j['name'])

data = []
for l1 in response.json()[0]['children']:
    l1_id, l1_name = l1['id'], l1['name']
    if 'children' in l1:
        for l2 in l1['children']:
            l2_id, l2_name = l2['id'], l2['name']
            if 'children' in l2:
                for l3 in l2['children']:
                    l3_id, l3_name = l3['id'], l3['name']
                    data.append({
                        'L1_ID': l1_id, 'L1_Name': l1_name,
                        'L2_ID': l2_id, 'L2_Name': l2_name,
                        'L3_ID': l3_id, 'L3_Name': l3_name
                    })
            else:
                data.append({
                    'L1_ID': l1_id, 'L1_Name': l1_name,
                    'L2_ID': l2_id, 'L2_Name': l2_name,
                    'L3_ID': None, 'L3_Name': None
                })
    else:
        data.append({
            'L1_ID': l1_id, 'L1_Name': l1_name,
            'L2_ID': None, 'L2_Name': None,
            'L3_ID': None, 'L3_Name': None
        })

# Convert the list to a DataFrame
df = pd.DataFrame(data)

# You can now print the DataFrame or save it to a CSV file
print(df)

df.to_csv('menu2.csv')
# df = pd.json_normalize(data=response.json()[0]['children'])
