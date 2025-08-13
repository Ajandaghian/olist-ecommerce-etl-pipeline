import pandas as pd

from config.log_config import get_logger
from config.config import config
from .base_cleaner import BaseDataCleaner, data_type_mapping

logger = get_logger(__name__)


class ProductsCleaner(BaseDataCleaner):
    """Products-specific cleaning logic."""

    def __init__(self, raw_data: pd.DataFrame, table_name: str = config["PRODUCTS_TABLE"]):
        super().__init__(raw_data, table_name)

    def clean(self) -> pd.DataFrame:
        """Main cleaning pipeline for products table."""
        logger.info("Starting products cleaning process")

        try:
            self.cleaned_data = self.raw_data.copy()
            self.cleaned_data.drop_duplicates(subset=['product_id'], keep='first', inplace=True)

            # Rename columns with typos
            self.cleaned_data.rename(columns={
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length"
            }, inplace=True)

            (self
                .data_type_validation(data_type_mapping.get(self.table_name)))

            logger.info("Products cleaning process completed")
            return self.cleaned_data

        except Exception as e:
            logger.error(f"Error during products cleaning: {str(e)}")
            raise

