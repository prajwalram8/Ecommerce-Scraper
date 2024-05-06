from carrefour.carrefour_mt_json import main as carrefour_main
from spinneys.spinneys_mt import main as spinneys_main
from choithrams.choithrams_mt import main as choithrams_main
from utils.logger import setup_logging
from db.sf_json_load_2 import jsonDataLoader as JSONDataLoader


# Initializing helper classes and functions
logger = setup_logging('main')

def preprocess_and_upload_carrefour(name = 'CARREFOUR'):
    """
    Function to invoke the multi-threaded scrape script and load 
    data into Snowflake
    """

    # Initializing helper classes
    local_stage = 'C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\carrefour'
    dc = JSONDataLoader()
    
    # Extraction and loading 
    if carrefour_main(local_stage=local_stage, num_workers=7):
        statement = '''
        JSON_DATA:"ean"::STRING as EAN,
        JSON_DATA:"id"::STRING as ID,
        JSON_DATA:"name"::STRING as NAME,
        JSON_DATA:"type"::STRING as TYPE,
        JSON_DATA:"category"[0]:"name"::STRING as CATEGORY_L1,
        JSON_DATA:"category"[1]:"name"::STRING as CATEGORY_L2,
        JSON_DATA:"category"[2]:"name"::STRING as CATEGORY_L3,
        JSON_DATA:"brand":"id"::STRING as BRAND_ID,
        JSON_DATA:"brand":"name"::STRING as BRAND_NAME,
        JSON_DATA:"foodType"::STRING as FOOD_TYPE,
        JSON_DATA:"price":"price"::FLOAT as PRICE,
        JSON_DATA:"price":"discount":"endDate"::STRING as DISCOUNT_END_DATE,
        JSON_DATA:"price":"discount":"price"::FLOAT as DISCOUNT_PRICE,
        JSON_DATA:"size"::STRING as SIZE,
        JSON_DATA:"unit":"itemsPerUnit"::STRING as ITEMS_PER_UNIT,
        JSON_DATA:"unitOfMeasure"::STRING as UNIT_OF_MEASURE,
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
        name='CARREFOUR', 
        local_stage_path=local_stage,
        select_statement=statement
        )
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        logger.error("Extraction Not Completed")
        return False
    

def preprocess_and_upload_spinneys(name = 'SPINNEYS'):
    """
    Function to invoke the multi-threaded scrape script and load 
    data into Snowflake
    """
    
    # Initializing helper classes
    local_stage = 'C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\spinneys'
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
        name='SPINNEYS', 
        local_stage_path=local_stage,
        select_statement=statement
        )
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        logger.error("Extraction Not Completed")
        return False
    

def preprocess_and_upload_choithrams(name = 'CHOITHRAMS'):
    """
    Function to invoke the multi-threaded scrape script and load 
    data into Snowflake
    """
    
    # Initializing helper classes
    local_stage = 'C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\choithrams'
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
        name='CHOITHRAMS', 
        local_stage_path=local_stage,
        select_statement=statement
        )
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        logger.error("Extraction Not Completed")
        return False


if __name__ == "__main__":
    if preprocess_and_upload_spinneys():
        logger.info("Spinneys loaded!")
    
    # if preprocess_and_upload_carrefour():
    #     logger.info("Carrefour loaded")

    if preprocess_and_upload_choithrams():
        logger.info("Choithrams Loaded!")


