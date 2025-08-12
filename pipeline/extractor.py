import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from config.config import config
from config.log_config import get_logger
from pipeline.base_db_connection import BaseDBConnection

logger = get_logger(__name__)

#TODO:
# - Unifying the connection logic for DataLoader
# - Add error handling for database connections and data extraction
# - Add logging instead of print statements
# - Ensure the schema exists before extracting data


class DataExtractor(BaseDBConnection):
    def __init__(self, source: str , *, file_paths: dict = None):
        """Initialize the DataExtractor with configuration and source.
        Args:
            source (str): The source of the data, e.g., 'CSV'
            file_paths (str | list, optional): Path to the CSV file(s) if source is 'CSV'.
                for example: {'orders': 'path/to/orders.csv', 'products': 'path/to/products.csv'}
        """

        if source not in ['CSV']:
            raise ValueError("Unsupported source type. Supported types are: 'CSV' ")
        self.source = source

        if source == 'CSV' and not file_paths:
            raise ValueError("file_paths must be provided for CSV source.")
        if isinstance(file_paths, dict):
            self.file_paths = file_paths

        self.connector = None
        load_dotenv()

    def _csv_extract_data(self) -> dict[str, pd.DataFrame]:
        """Extract data from a CSV file."""

        dataframes = {}
        try:
            for name, path in self.file_paths.items():
                dataframes[name] = pd.read_csv(path)
            return dataframes
        except Exception as e:
            raise ValueError(f"Error reading {name}, {path}: {e}")


    def extract(self) -> dict[str, pd.DataFrame]:
        """Extract data based on the source type."""

        try:
            if self.source == 'CSV':
                return self._csv_extract_data()
        except Exception as e:
            print(f"Error extracting data: {e}")
            raise e
        finally:
            self._close_connection()

