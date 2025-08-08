from config.config import config
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

#TODO:
# - Implement the logic to load data into PostgreSQL
# - Add error handling for database connections and data loading
# - Add logging instead of print statements
# - Checking for empty DataFrames before loading
# - Ensure the schema exists before loading data


class DataLoader():
    def __init__(self, source: str, *, dataframe_table_mapping: dict, schema: str):
        """Initialize the DataLoader with configuration and source.
        **Make sure you have created the schema in the target database before loading data.**

        Args:
            source (str): The source of the data, e.g., 'Postgres', 'Snowflake'.
            dataframe (pd.DataFrame | list): The DataFrame or list of DataFrames containing the data to be loaded.
            dataframe_table_mapping (dict): A mapping of DataFrame names to target table.
                the keys are the dataframe and the values are the table names.
                    {table_name: dataframe}
        """
        load_dotenv()
        if source not in ['Postgres', 'Snowflake']:
            raise ValueError("Unsupported source type. Supported types are: 'Postgres', 'Snowflake'.")
        self.source = source
        self.dataframe_table_mapping = dataframe_table_mapping
        self.schema = schema
        self.connector = None

    def _connection(self):
        """Create a connection to the data source."""

        if self.source == 'Postgres':
            engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
            self.connector = engine
            return engine

        elif self.source == 'Snowflake':
            engine = create_engine(URL(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
            ))

            self.connector = engine
            return engine

    def _close_connection(self):
        if self.connector is not None:
            if hasattr(self.connector, 'dispose'):
                self.connector.dispose()
            else:
                self.connector.close()

    def _postgres_load_data(self):
        pass

    def _snowflake_load_data(self):
        """Load data into Snowflake."""

        if self.connector is None:
            self.connector = self._connection()

        for table_name, df in self.dataframe_table_mapping.items():
            with self.connector.begin() as conn:
                df.to_sql(
                    table_name,
                    con=conn,
                    schema=self.schema,
                    if_exists='append',
                    index=False,
                    chunksize=config['CHUNK_SIZE']
                )
                print(f"Data loaded into {table_name} table in Snowflake.")

    def _csv_load_data(self):
        """Load data from CSV files into the target database."""
        DIR = config['CLEANED_DATA_DIR']
        for df, table_name in self.dataframe_table_mapping.items():
            df.to_csv(f"{DIR}/{table_name}.csv", index=False)
            print(f"Data saved to {table_name}.csv in {DIR} directory.")

    def load_data(self):
        """Load data into the target database."""

        if self.source == 'Snowflake':
            self._snowflake_load_data()

        elif self.source == 'CSV':
            self._csv_load_data()

        else:
            raise ValueError("Unsupported source type")

        self._close_connection()