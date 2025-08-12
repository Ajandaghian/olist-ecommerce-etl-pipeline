# Main orchestrator for data cleaning operations"
from typing import Dict, Type

import numpy as np
import pandas as pd

from config.config import config
from config.log_config import get_logger

from .data_processors.base_cleaner import BaseDataCleaner
from .data_processors.orders_table_cleaner import OrdersCleaner
from .data_processors.customers_table_cleaner import CustomersCleaner

logger = get_logger(__name__)

class DataCleaningFactory:
    """Factory for creating cleaners."""

    cleaner_map: Dict[str, Type[BaseDataCleaner]] = {
        "Customers": CustomersCleaner,
        "Geolocation": BaseDataCleaner,

        "Orders": OrdersCleaner,
        "Order_Items": BaseDataCleaner,
        "Order_Payments": BaseDataCleaner,
        "Order_Reviews": BaseDataCleaner,

        "Products": BaseDataCleaner,
        "Product_Category_Name_Translation": BaseDataCleaner,

        "Sellers": BaseDataCleaner
    }





    @classmethod
    def create_cleaner(cls, table_name: str, dataframe: pd.DataFrame) -> BaseDataCleaner:
        """Create appropriate cleaner for the table."""

        cleaner_class = cls.cleaner_map.get(table_name)
        if not cleaner_class:
            raise ValueError(f"No cleaner available for table: {table_name}")

        return cleaner_class(raw_data=dataframe, table_name=table_name)

class DataCleaningPipeline:
    """ Main orchestrator for all cleaning operations.
        dataframes: A dictionary of DataFrames to clean.
            example: {
                'orders': pd.DataFrame(...),
                'customers': pd.DataFrame(...),
            }
    """

    def __init__(self, dataframes: dict[str, pd.DataFrame]):
        self.dataframes = dataframes
        self.cleaned_dataframes = {}

    def run(self) -> dict[str, pd.DataFrame]:
        """Execute the data cleaning pipeline."""
        for table_name, df in self.dataframes.items():
            logger.info(f"Cleaning data for table: {table_name}")
            cleaner = DataCleaningFactory.create_cleaner(table_name=table_name, dataframe=df)
            self.cleaned_dataframes[table_name] = cleaner.clean()

        logger.info("Data cleaning pipeline completed successfully.")
        return self.cleaned_dataframes



if __name__ == "__main__":

    import pipeline.extractor as ext
    import pipeline.loader as load

    # Example: Run the data cleaning pipeline
    raw_dataframes = ext.DataExtractor(
            source='CSV',
            file_paths={
                'Orders': config['RAW_DATA_DIR'] + config['ORDERS_PATH'],

            }
    ).extract()

    data_cleaning_pipeline = DataCleaningPipeline(dataframes=raw_dataframes)
    result = data_cleaning_pipeline.run()
    print(result.get('Orders').dtypes)
    print(result.get('Orders'))

    loader = load.DataLoader(
        source='snowflake',
        dataframe_table_mapping=result,
        schema='test'
        ).load_data()