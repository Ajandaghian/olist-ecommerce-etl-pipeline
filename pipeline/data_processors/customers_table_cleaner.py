# data_processors/customers_table_cleaner.py
"""Cleaner specific for customers table."""


import pandas as pd

from config.log_config import get_logger
from .base_cleaner import BaseDataCleaner, data_type_mapping

logger = get_logger(__name__)


class CustomersCleaner(BaseDataCleaner):
    """Customers-specific cleaning logic."""

    def __init__(self, raw_data: pd.DataFrame, table_name: str = "Customers"):
        super().__init__(raw_data, table_name)

    def clean(self) -> pd.DataFrame:
        """Main cleaning pipeline for customers table."""
        logger.info("Starting customers cleaning process")

        try:
            # Execute cleaning pipeline step by step
            self.cleaned_data = self.raw_data.copy()
            self.cleaned_data = self.cleaned_data.drop_duplicates(subset=['customer_id'], keep='first')

            (self
                .data_type_validation(data_type_mapping.get(self.table_name)))

            logger.info("Customers cleaning process completed")
            return self.cleaned_data

        except Exception as e:
            logger.error(f"Error during customers cleaning: {str(e)}")
            raise

