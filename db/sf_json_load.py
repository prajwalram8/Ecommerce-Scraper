from contextlib import contextmanager
from typing import Tuple, Dict, Optional
from utils.utils import has_json_files
import snowflake.connector
import datetime as dt
from utils.logger import setup_logging
from credentials.credential_manager import CredentialManager

# Set up logging
logger = setup_logging(name='snowflake_loader')

class DataLoader:
    """
    A class to load data into Snowflake using Python best practices 
    improved error handling, and a Pythonic approach to resource 
    management and documentation.
    """
    def __init__(self) -> None:

        # Initializing helper objects
        self.credentials = CredentialManager()

        # Classes level initializations
        self.conn_details = self.prepare_conn_details()
        self.timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        self.stage_name: Optional[str] = None
        self.table_name: Optional[str] = None
        self.snowflake_database = self.conn_details['database']
        self.snowflake_schema = self.conn_details['schema']
        

    def prepare_conn_details(self) -> Dict[str, str]:
        """Prepare the connection details using environment variables or config file settings."""
        details = self.credentials.get_credentials(
            user = 'snowflake',
            password = 'snowflake',
            account = 'snowflake',
            warehouse = 'snowflake',
            database = 'snowflake',
            schema = 'snowflake',
            role = 'snowflake'
        )
        return details

    @contextmanager
    def snowflake_connection(self):
        """
        Context manager for Snowflake connection, 
        ensuring it's closed after use.
        """
        
        conn = snowflake.connector.connect(
            user=self.conn_details['user'],
            password=self.conn_details['password'],
            account=self.conn_details['account'],
            warehouse=self.conn_details['warehouse'],
            database=self.conn_details['database'],
            schema=self.conn_details['schema'],
            role=self.conn_details['role'],
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str) -> str:
        """
        Execute a query against the 
        Snowflake database and return the query ID.
        """
        with self.snowflake_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                return cursor.sfqid
            finally:
                cursor.close()

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the Snowflake schema
        ."""
        query = f"""SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{self.snowflake_schema.upper()}' 
                    AND TABLE_NAME = '{table_name.upper()}';"""
        with self.snowflake_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] > 0
            finally:
                cursor.close()

    def local_stage_sf_stage(self, name: str, local_stage_path: str, replace: bool = False) -> str:
        """
        Create or replace a Snowflake stage, load data 
        Returns the the put query ID.
        """
        name = name.upper()
        stage_name = f'{name}_STAGE'
        table_name = f'{name}_TABLE'

        if replace:
            stage_name += f'_{self.timestamp}'
        
        if has_json_files(folder_path=local_stage_path):
            stage_create = f"""CREATE OR REPLACE STAGE {self.snowflake_database+'.'+self.snowflake_schema+'.'+stage_name}"""
            self.execute_query(stage_create)

            local_stage_path = local_stage_path.replace('\\', '/')
            put_command = f"PUT file://{local_stage_path}/*.json @{stage_name};" if ' ' not in local_stage_path else f"PUT 'file://{local_stage_path}/*.json' @{stage_name};"
            put_qid = self.execute_query(put_command)

            self.stage_name = stage_name
            self.table_name = table_name

            return put_qid
        else:
            logger.warning("No CSV files in the local stage folder")
            return ""

    def file_format(self) -> str:
        """Create or replace the file format for CSV uploads and return the query ID."""
        file_format_handling = '''
            CREATE OR REPLACE FILE FORMAT JSON_LOAD_FORMAT
            TYPE=JSON
            STRIP_OUTER_ARRAY = TRUE
            '''
        return self.execute_query(file_format_handling)

    def create_table(self,  temp_table: bool = False) -> Tuple[str, str]:
        """Create a table in Snowflake and return the query ID and table name."""
        table_name = f"TEMP_{self.table_name}" if temp_table else self.table_name
        create_table_query = f"""CREATE OR REPLACE TABLE {self.snowflake_database+'.'+self.snowflake_schema+'.'+table_name} (JSON_DATA VARIANT);"""
        ct_qid = self.execute_query(create_table_query)
        return ct_qid, table_name

    def copy_into(self, temp_table: bool = False, explicit_stage: Optional[str] = None) -> Tuple[str, str]:
        """Copy data from a stage into a Snowflake table and return the query ID and table name."""
        stage_name = explicit_stage if explicit_stage else self.stage_name
        if not stage_name:
            raise ValueError("No stage defined!")
        
        _, table_name = self.create_table(temp_table=temp_table)
        copy_command = f'''COPY INTO {table_name} FROM @{stage_name} FILE_FORMAT = (FORMAT_NAME = JSON_LOAD_FORMAT);'''
        copy_qid = self.execute_query(copy_command)
        return copy_qid, table_name

    def insert_into(self) -> Tuple[str, str]:
        """Insert data into a table from a temporary table, then drop the temporary table."""
        if not self.table_name:
            logger.error("Table name is not set")
            return "", ""

        _, temp_table_name = self.copy_into(temp_table=True)
        insert_query = f'''INSERT INTO {self.snowflake_database}.{self.snowflake_schema}.{self.table_name} SELECT * FROM {temp_table_name};'''
        insert_qid = self.execute_query(insert_query)
        drop_query = f"DROP TABLE IF EXISTS {temp_table_name};"
        drop_qid = self.execute_query(drop_query)
        return insert_qid, drop_qid

    def truncate_table(self) -> str:
        """Truncate the table in Snowflake and return the query ID."""
        truncate_query = f"TRUNCATE TABLE {self.snowflake_database}.{self.snowflake_schema}.{self.table_name};"
        return self.execute_query(truncate_query)

    def manage_data_loading(self,name: str,  local_stage_path: str, load_type: str = 'truncate') -> None:
        """
        Manages data loading by checking if the table exists, and based on the operation type,
        it either truncates, inserts, or creates a new table and loads data into it.

        Parameters:
        - local_stage_path: The local directory path containing CSV files to load.
        - col_def_str: Column definition string for creating a new table, if necessary.
        - load_type: The type of load operation ('truncate', 'insert'). Defaults to 'insert'.
        """
        self.local_stage_sf_stage(name=name, local_stage_path=local_stage_path)
        self.file_format()

        # Check if the table exists
        if self.table_exists(self.table_name):
            if load_type == 'truncate':
                # Truncate the table before loading data
                logger.info(f"Truncating table {self.table_name} before loading data.")
                self.truncate_table()
                self.copy_into(temp_table=False)
            elif load_type == 'insert':
                # Insert data into the table
                logger.info(f"Inserting data into table {self.table_name}.")
                self.insert_into()
            else:
                logger.error("Invalid load type specified. Only 'truncate' and 'insert' are supported.")
        else:
            # Table does not exist, create it and then load data
            logger.info(f"Table {self.table_name} does not exist. Creating table and loading data.")
            self.copy_into(temp_table=False)

if __name__ == "__main__":
    dataloader = DataLoader()
    dataloader.manage_data_loading(
        name='SAMPLE', 
        local_stage_path="C:\\Users\\Prajwal.G\\Documents\\POC\\Ecom Scraper\\data\\carrefour",
        load_type='insert'
        )