import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from config.config import config
from config.log_config import get_logger

logger = get_logger(__name__)

#TODO:
# - Implement the logic to extract data from PostgreSQL
# - Unifying the connection logic for DataLoader
# - Add error handling for database connections and data extraction
# - Add logging instead of print statements
# - Ensure the schema exists before extracting data


class DataExtractor():
    def __init__(self, source: str , *, file_paths: dict = None):
        """Initialize the DataExtractor with configuration and source.
        Args:
            source (str): The source of the data, e.g., 'CSV'
            file_paths (str | list, optional): Path to the CSV file(s) if source is 'CSV'.
                for example: {'orders': 'path/to/orders.csv', 'products': 'path/to/products.csv'}
        """

        if source not in ['CSV']:   #, 'Postgres', 'Snowflake']:
            raise ValueError("Unsupported source type. Supported types are: 'CSV' ") #'Postgres', 'Snowflake'.")
        self.source = source

        if source == 'CSV' and not file_paths:
            raise ValueError("file_paths must be provided for CSV source.")
        if isinstance(file_paths, dict):
            self.file_paths = file_paths

        self.connector = None
        load_dotenv()

    def _connection(self):
        """Create a connection to the data source."""

        if self.source == 'CSV':
            # For CSV, no connection is needed, just return None
            self.connector = None
            return None

        if self.source == 'Postgres':
            engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
            self.connector = engine
            return engine

        elif self.source == 'Snowflake':
            conn = create_engine(
                        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
                            user=os.getenv("SNOWFLAKE_USER"),
                            password=os.getenv("SNOWFLAKE_PASSWORD"),
                            account=os.getenv("SNOWFLAKE_ACCOUNT"),
                            database=os.getenv("SNOWFLAKE_DATABASE"),
                            schema=os.getenv("SNOWFLAKE_SCHEMA"),
                            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
                        )
                    )

            self.connector = conn
            return conn

        else:
            raise ValueError("Unsupported source type")

    def _close_connection(self):
        if self.connector is not None:
            if hasattr(self.connector, 'dispose'):
                self.connector.dispose()
            else:
                self.connector.close()


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

