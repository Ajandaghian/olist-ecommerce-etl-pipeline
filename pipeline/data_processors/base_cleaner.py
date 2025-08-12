# data_processors/base_cleaner.py
"""Abstract base class for all data cleaners."""

from abc import ABC, abstractmethod
from typing import List
import pandas as pd

from config.log_config import get_logger

logger = get_logger(__name__)


data_type_mapping = {
    'Orders': {
        'order_id': 'string',
        'customer_id': 'string',
        'order_status': 'category',
        'order_purchase_timestamp': 'datetime64[ns]',
        'order_approved_at': 'datetime64[ns]',
        'order_delivered_carrier_date': 'datetime64[ns]',
        'order_delivered_customer_date': 'datetime64[ns]',
        'order_estimated_delivery_date': 'datetime64[ns]'
    },

    'Customers': {
        'customer_id': 'string',
        'customer_unique_id': 'string',
        'customer_zip_code_prefix': 'int64',
        'customer_city': 'string',
        'customer_state': 'string',
        'created_at': 'datetime64[ns]'
    },

    'Products': {
        'product_id': 'string',
        'product_category_name': 'string',
        'product_name_length': 'Int64',
        'product_description_length': 'Int64',
        'product_photos_qty': 'Int64',
        'product_weight_g': 'Int64',
        'product_length_cm': 'Int64',
        'product_height_cm': 'Int64',
        'product_width_cm': 'Int64'
    },

    'Sellers': {
        'seller_id': 'string',
        'seller_zip_code_prefix': 'Int64',
        'seller_city': 'string',
        'seller_state': 'string'
    },

    'OrderItems': {
        'order_id': 'string',
        'order_item_id': 'int64',
        'product_id': 'string',
        'seller_id': 'string',
        'shipping_limit_date': 'datetime64[ns]',
        'price': 'float64',
        'freight_value': 'float64'
    },

    'OrderPayments': {
        'order_id': 'string',
        'payment_sequential': 'int64',
        'payment_type': 'string',
        'payment_installments': 'int64',
        'payment_value': 'float64'
    },

    'OrderReviews': {
        'review_id': 'string',
        'order_id': 'string',
        'review_score': 'int64',
        'review_comment_title': 'string',
        'review_comment_message': 'string',
        'review_creation_date': 'datetime64[ns]',
        'review_answer_timestamp': 'datetime64[ns]'
    },

    'Geolocation': {
        'geolocation_zip_code_prefix': 'int64',
        'geolocation_lat': 'float64',
        'geolocation_lng': 'float64',
        'geolocation_city': 'string',
        'geolocation_state': 'string'
    },

    'ProductCategoryNameTranslation': {
        'product_category_name': 'string',
        'product_category_name_english': 'string'
    }
}


class BaseDataCleaner(ABC):
    def __init__(self, raw_data: pd.DataFrame, table_name: str):
        self.raw_data = raw_data.copy()
        self.cleaned_data = raw_data.copy()
        self.table_name = table_name

    def data_type_validation(self, mapping: dict):
        for column, expected_type in mapping.items():
            if column in self.cleaned_data.columns:
                actual_type = self.cleaned_data[column].dtype
                if actual_type != expected_type:
                    try:
                        self.cleaned_data[column] = self.cleaned_data[column].astype(expected_type)
                    except ValueError:
                        logger.error(f"Column '{column}' expected type {expected_type}, but got {actual_type}.")
                        raise
        logger.debug("Data type validation completed successfully.")
        return self

    @abstractmethod
    def clean(self) -> pd.DataFrame:
        """Must be implemented by subclasses."""
        pass

    def clean(self):
        self.cleaned_data = self.raw_data.copy()
        self.cleaned_data = self.cleaned_data.drop_duplicates(keep='first')

        (self
            .data_type_validation(data_type_mapping.get(self.table_name)))

        return self.cleaned_data