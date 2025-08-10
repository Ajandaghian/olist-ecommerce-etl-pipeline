from etl_pipeline import ETLPipeline
import os
from dotenv import load_dotenv

load_dotenv()


### it's possible to add logics related to run this pipeline with intervals
if __name__ == "__main__":

    kwargs = {
        'extractor_pipeline_args': {
            'extractor_source': os.getenv('EXTRACT_SOURCE'),
            'file_paths': {
                        # 'customers': os.getenv('CUSTOMERS_PATH'),
                        #'geolocation': os.getenv('GEOLOCATION_PATH'),
                        'orders': os.getenv('ORDERS_PATH'),
                        # 'order_items': os.getenv('ORDER_ITEMS_PATH'),
                        # 'order_payments': os.getenv('ORDER_PAYMENTS_PATH'),
                        # 'order_reviews': os.getenv('ORDER_REVIEWS_PATH'),
                        # 'products': os.getenv('PRODUCTS_PATH'),
                        # 'categories': os.getenv('CATEGORIES_PATH'),
                        # 'sellers': os.getenv('SELLERS_PATH')
                            }
                },
        'loading_pipeline_args': {
            'source': os.getenv('LOAD_SOURCE'),
            'schema': os.getenv('LOAD_SCHEMA')
                }
    }


    ETLPipeline(**kwargs).run()



