from config.config import config
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import pathlib

DIR = pathlib.Path(__file__).parent.parent

# Load environment variables
load_dotenv()
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

print(f"Connecting to database {database} at {host}:{port} as user {user}")


def load_csv_to_postgres(csv_file, table_name):
    """Load a CSV file into a PostgreSQL table."""
    # Create a database connection
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    # Load CSV into DataFrame
    df = pd.read_csv(csv_file)

    # Write DataFrame to PostgreSQL table
    df.to_sql(table_name, engine, schema='raw', if_exists='append', index=False)
    print(f"Data from {csv_file} loaded into {table_name} table.")


def batch_load(file_to_table_map: dict):


    for csv_file, table_name in file_to_table_map.items():
        load_csv_to_postgres(csv_file, table_name)


# == Main execution block ===
if __name__ == "__main__":
    csv_files = {
        config['RAW_DATA_DIR'] + config['CUSTOMERS_PATH']: "customers",
        config['RAW_DATA_DIR'] + config['GEOLOCATION_PATH']: "geolocation",
        config['RAW_DATA_DIR'] + config['ORDERS_PATH']: "orders",
        config['RAW_DATA_DIR'] + config['ORDER_ITEMS_PATH']: "order_items",
        config['RAW_DATA_DIR'] + config['ORDER_PAYMENTS_PATH']: "order_payments",
        config['RAW_DATA_DIR'] + config['ORDER_REVIEWS_PATH']: "order_reviews",
        config['RAW_DATA_DIR'] + config['PRODUCTS_PATH']: "products",
        config['RAW_DATA_DIR'] + config['CATEGORIES_PATH']: "product_category_name_translation",
        config['RAW_DATA_DIR'] + config['SELLERS_PATH']: "sellers"
    }

    batch_load(csv_files)
