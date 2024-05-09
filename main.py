import os
from datetime import datetime as dt
from spinneys.spinneys_mt import main as spinneys_main
from choithrams.choithrams_mt import main as choithrams_main
from carrefour.carrefour_sel_mp_json import run_parallel_extraction as carrefour_main
from utils.utils import create_directory, delete_folder_contents
from utils.logger import setup_logging
from utils.spaces_upload import FolderUploader
from db.sf_json_load import jsonDataLoader as JSONDataLoader

# Initializing helper classes and functions
logger = setup_logging('main')
uploader = FolderUploader('config.ini')


# Path Dependencies
data_stage_folder = os.path.join(os.getcwd(), 'data')
create_directory(data_stage_folder)

# CARREFOUR
def preprocess_and_upload_carrefour(name = 'CARREFOUR'):
    """
    Function to invoke the multi-threaded scrape script and load 
    data into Snowflake
    """

    categories = [
        'F1600000','F11600000','F1700000','F1500000','F6000000',
        'F1610000','F1200000','NF3000000','NF2000000','F1000000'
        ]

    # Initializing helper classes
    local_stage = os.path.join(data_stage_folder,f'{name}')
    create_directory(local_stage)
    dc = JSONDataLoader()
    
    # Extraction and loading 
    if carrefour_main(categories= categories, stage_path=local_stage):
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
        name=name, 
        local_stage_path=local_stage,
        select_statement=statement
        )
        uploader.upload_folder(local_stage, 'ecommerceScraping', f'{name}/{dt.strftime(dt.now(), "%Y%m%d")}')
        delete_folder_contents(folder_path=local_stage)
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        logger.error("Extraction Not Completed")
        return False
    

# SPINNEYS
def preprocess_and_upload_spinneys(name = 'SPINNEYS'):
    """
    Function to invoke the multi-threaded scrape script and load 
    data into Snowflake
    """
    
    # Initializing helper classes
    local_stage = local_stage = os.path.join(data_stage_folder,f'{name}')
    create_directory(local_stage)
    dc = JSONDataLoader()

    # Extraction and loading 
    if spinneys_main(local_stage=local_stage, num_workers=7):
        statement = '''
        JSON_DATA:"id"::STRING as EAN,
        JSON_DATA:"item_name"::STRING as NAME,
        JSON_DATA:"item_price"::FLOAT as PRICE,
        JSON_DATA:"item_link"::STRING as LINK,
        JSON_DATA:"item_quantity"::STRING as QUANTITY,
        CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
        '''
        dc.manage_data_loading(
        name=name, 
        local_stage_path=local_stage,
        select_statement=statement
        )
        uploader.upload_folder(local_stage, 'ecommerceScraping', f'{name}/{dt.strftime(dt.now(), "%Y%m%d")}')
        delete_folder_contents(folder_path=local_stage)
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        logger.error("Extraction Not Completed")
        return False
    

# CHOITHRAMS
def preprocess_and_upload_choithrams(name = 'CHOITHRAMS'):
    """
    Function to invoke the multi-threaded scrape script and load 
    data into Snowflake
    """
    
    # Initializing helper classes
    local_stage = local_stage = os.path.join(data_stage_folder,f'{name}')
    create_directory(local_stage)
    dc = JSONDataLoader()

    # Extraction and loading 
    if choithrams_main(local_stage=local_stage,num_workers=7):
        statement = '''
        JSON_DATA:"item_id"::STRING as EAN,
        JSON_DATA:"item_name"::STRING as NAME,
        JSON_DATA:"price"::FLOAT as PRICE,
        JSON_DATA:"item_category"::STRING as CATEGORY,
        JSON_DATA:"item_brand"::STRING as BRAND,
        JSON_DATA:"quantity"::FLOAT as QUANTITY,
        CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
        '''
        dc.manage_data_loading(
        name=name, 
        local_stage_path=local_stage,
        select_statement=statement
        )
        uploader.upload_folder(local_stage, 'ecommerceScraping', f'{name}/{dt.strftime(dt.now(), "%Y%m%d")}')
        delete_folder_contents(folder_path=local_stage)
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        logger.error("Extraction Not Completed")
        return False


if __name__ == "__main__":
    if preprocess_and_upload_spinneys():
        logger.info("Spinneys loaded!")

    if preprocess_and_upload_choithrams():
        logger.info("Choithrams Loaded!")

    if preprocess_and_upload_carrefour():
        logger.info("Carrefour loaded")


