from carrefour.carrefour_mt import main as carrefour_main
from spinneys.spinneys_mt import main as spinneys_main
from utils.logger import setup_logging
from data_processor.data_processor import LocalStageOrchestrator
from utils.utils import ProjectDirectory
import os
from db.snowflake_loader import DataLoader

# Initializing helper classes and functions
logger = setup_logging('main')

def preprocess_and_upload_carrefour(name = 'carrefour', load_type='insert'):
    # Initializing helper classes
    pdc = ProjectDirectory(name=name, root=os.path.join(os.getcwd(), 'data'))
    dc = DataLoader()
    local_stage_orchestrator = LocalStageOrchestrator(
        staging_location=pdc.get_directories('snowflake_stage')
    )
    
    # Setting up project directory
    local_stage = pdc.get_directories(name)
    snowflake_stage = pdc.get_directories('snowflake_stage')

    # Extraction and loading 
    if carrefour_main(local_stage=local_stage, num_workers=10):
        col_definition_string = local_stage_orchestrator.process_flat_files(local_stage)
        dc.manage_data_loading(name=name, local_stage_path=snowflake_stage, col_def_str=col_definition_string, load_type=load_type)
        local_stage_orchestrator.delete_folder_contents(folder_path=local_stage)
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        local_stage_orchestrator.delete_folder_contents(folder_path=local_stage)
        logger.warn("Extraction Not Completed")
        return False
    

def preprocess_and_upload_spinneys(name = 'spinneys', load_type='insert'):
    # Initializing helper classes
    pdc = ProjectDirectory(name=name, root=os.path.join(os.getcwd(), 'data'))
    dc = DataLoader()
    local_stage_orchestrator = LocalStageOrchestrator(
        staging_location=pdc.get_directories('snowflake_stage')
    )
    
    # Setting up project directory
    local_stage = pdc.get_directories(name)
    snowflake_stage = pdc.get_directories('snowflake_stage')

    # Extraction and loading 
    if spinneys_main(local_stage=local_stage, num_workers=10):
        col_definition_string = local_stage_orchestrator.process_flat_files(local_stage)
        dc.manage_data_loading(name=name, local_stage_path=snowflake_stage, col_def_str=col_definition_string, load_type=load_type)
        local_stage_orchestrator.delete_folder_contents(folder_path=local_stage)
        logger.info(f"Extraction, Preprocessing & Upload of {name} has been completed.")
        return True
    else:
        local_stage_orchestrator.delete_folder_contents(folder_path=local_stage)
        logger.warn("Extraction Not Completed")
        return False


if __name__ == "__main__":
    if preprocess_and_upload_spinneys():
        logger.info("loaded!")

    
        # logger.info("data extracted and loaded")

