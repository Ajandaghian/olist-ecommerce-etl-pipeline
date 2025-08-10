from config.config import config
from pipeline.loader import DataLoader
from pipeline.extractor import DataExtractor


if __name__ == "__main__":
    SCHEMA = 'RAW'

    table_paths_mapping = {
        "customers": config['RAW_DATA_DIR'] + config['CUSTOMERS_PATH'],
        "geolocation": config['RAW_DATA_DIR'] + config['GEOLOCATION_PATH'],
        "orders": config['RAW_DATA_DIR'] + config['ORDERS_PATH'],
        "order_items": config['RAW_DATA_DIR'] + config['ORDER_ITEMS_PATH'],
        "order_payments": config['RAW_DATA_DIR'] + config['ORDER_PAYMENTS_PATH'],
        "order_reviews": config['RAW_DATA_DIR'] + config['ORDER_REVIEWS_PATH'],
        "products": config['RAW_DATA_DIR'] + config['PRODUCTS_PATH'],
        "product_category_name_translation": config['RAW_DATA_DIR'] + config['CATEGORIES_PATH'],
        "sellers": config['RAW_DATA_DIR'] + config['SELLERS_PATH']
    }

    data = DataExtractor(source='CSV', file_paths=table_paths_mapping).extract()

    DataLoader(
        source='snowflake',
        dataframe_table_mapping=data,
        schema=SCHEMA).load_data()

