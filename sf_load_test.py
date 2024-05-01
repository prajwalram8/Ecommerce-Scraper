from db.sf_json_load_2 import jsonDataLoader as JSONDataLoader
import json
import os

products = []
load_stage = "C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\dev_stage"

with open("first_load_products.json") as f:
    first_load = json.load(f)

with open("products.json") as f:
    others = json.load(f)

products.extend(first_load)
products.extend(others)

with open(os.path.join(load_stage,"final_for_upload.json"), 'w', encoding='utf-8') as f:
    json.dump(products, f, ensure_ascii=False, indent=4)

dc = JSONDataLoader()

statement = '''
JSON_DATA:"ean"::STRING as EAN,
COALESCE(JSON_DATA:"id"::STRING, JSON_DATA:"productId"::STRING) as ID,
JSON_DATA:"name"::STRING as NAME,
JSON_DATA:"type"::STRING as TYPE,
COALESCE(JSON_DATA:"category"[0]:"name"::STRING, JSON_DATA:"categories"[0]:"name"::STRING)  as CATEGORY_L1,
COALESCE(JSON_DATA:"category"[1]:"name"::STRING, JSON_DATA:"categories"[1]:"name"::STRING)  as CATEGORY_L2,
COALESCE(JSON_DATA:"category"[2]:"name"::STRING, JSON_DATA:"categories"[2]:"name"::STRING)  as CATEGORY_L3,
JSON_DATA:"brand":"id"::STRING as BRAND_ID,
COALESCE(JSON_DATA:"brand":"name"::STRING, JSON_DATA:"brand"::STRING)  as BRAND_NAME,
JSON_DATA:"foodType"::STRING as FOOD_TYPE,
COALESCE(JSON_DATA:"price":"price"::FLOAT, JSON_DATA:"applicablePrice"::FLOAT) as PRICE,
COALESCE(JSON_DATA:"price":"discount":"endDate"::STRING, JSON_DATA:"discount":"endDate"::STRING) as DISCOUNT_END_DATE,
COALESCE(JSON_DATA:"price":"discount":"price"::FLOAT, JSON_DATA:"discount":"price"::FLOAT) as DISCOUNT_PRICE,
JSON_DATA:"size"::STRING as SIZE,
COALESCE(JSON_DATA:"unit":"itemsPerUnit"::STRING, JSON_DATA:"itemsPerUnit"::STRING) as ITEMS_PER_UNIT,
COALESCE(JSON_DATA:"unit":"unitOfMeasure"::STRING, JSON_DATA:"unitOfMeasure"::STRING) as UNIT_OF_MEASURE,
JSON_DATA:"isMarketPlace"::STRING  as IS_MARKETPLACE,
JSON_DATA:"productOrigin"::STRING as PRODUCT_ORIGIN,
JSON_DATA:"promoBadges"[0]:"text":"boldText"::STRING as PROMO_BADGE_1,
JSON_DATA:"promoBadges"[1]:"text":"boldText"::STRING as PROMO_BADGE_2,
JSON_DATA:"promoBadges"[2]:"text":"boldText"::STRING as PROMO_BADGE_3,
JSON_DATA:"stock":"stockLevelStatus"::STRING as STOCK_LEVEL_STATUS,
JSON_DATA:"availability":"isAvailable"::STRING as IS_AVAILABLE,
CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
'''

dc.manage_data_loading(
    name="DEV_TEST",
    local_stage_path=load_stage,
    select_statement=statement

)