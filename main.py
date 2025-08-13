from etl_pipeline import ETLPipeline
from config.config import config

import os
from dotenv import load_dotenv

load_dotenv()


### it's possible to add logics related to run this pipeline with intervals
if __name__ == "__main__":

    kwargs = {
        'extractor_pipeline_args': {
            'extractor_source': os.getenv('EXTRACT_SOURCE'),
            'file_paths': {
                        config['CUSTOMERS_TABLE']: os.getenv('CUSTOMERS_PATH'),
                        config['GEOLOCATION_TABLE']: os.getenv('GEOLOCATION_PATH'),
                        config['ORDERS_TABLE']: os.getenv('ORDERS_PATH'),
                        config['ORDER_ITEMS_TABLE']: os.getenv('ORDER_ITEMS_PATH'),
                        config['ORDER_PAYMENTS_TABLE']: os.getenv('ORDER_PAYMENTS_PATH'),
                        config['ORDER_REVIEWS_TABLE']: os.getenv('ORDER_REVIEWS_PATH'),
                        config['PRODUCTS_TABLE']: os.getenv('PRODUCTS_PATH'),
                        config['CATEGORIES_TABLE']: os.getenv('CATEGORIES_PATH'),
                        config['SELLERS_TABLE']: os.getenv('SELLERS_PATH')
                            }
                },
        'loading_pipeline_args': {
            'source': os.getenv('LOAD_SOURCE'),
            'schema': os.getenv('LOAD_SCHEMA')
                }
    }


    ETLPipeline(**kwargs).run()



