
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

import os
from dotenv import load_dotenv

load_dotenv()

class BaseDBConnection:
    def __init__(self, source: str):
        self.source = source
        self.connector = None

    def _connection(self):
        """Create a connection to the data source."""

        if self.source == 'CSV':
            self.connector = None
            return None

        if self.source == 'postgres':
            engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
            self.connector = engine
            return engine

        elif self.source == 'snowflake':
            engine = create_engine(URL(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
            ))

            engine = create_engine(
                'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
                    user=os.getenv("SNOWFLAKE_USER"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
                )
            )

            self.connector = engine
            return engine

    def _close_connection(self):
        if self.connector is not None:
            if hasattr(self.connector, 'dispose'):
                self.connector.dispose()
            else:
                self.connector.close()
