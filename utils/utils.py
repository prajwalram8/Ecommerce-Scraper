import os
from .logger import setup_logging

# Initializing helper classes and functions
logger = setup_logging(__name__)

def create_directory(dir_path):
    try:
        os.makedirs(dir_path)
    except FileExistsError:
        # directory already exists
        pass

def has_json_files(folder_path):
        """
        Check if the given folder contains any JSON files
        """
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            # Check if the file is a CSV file
            if file_name.endswith('.json') and os.path.isfile(file_path):
                return True

        # No CSV files were found
        return False

def py_file_name():
    return os.path.basename(__file__).replace('.py','')


def delete_folder_contents(folder_path):
    """
    Recursively delete the contents of a folder.
    """
    try:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isdir(file_path):
                delete_folder_contents(file_path)
                os.rmdir(file_path)
            else:
                os.remove(file_path)
        logger.info(f"Folder content of: {folder_path} deleted!")
    except Exception as e:
        logger.error(f"Error while deleting folder contents: {e}", exc_info=True)
        raise