# Main orchestrator for data cleaning operations"
from typing import Dict, Type

import numpy as np
import pandas as pd

import pipeline.extractor as ext
import pipeline.loader as load
from config.config import config
from config.log_config import get_logger

from .data_processors.base_cleaner import BaseDataCleaner, PassCleaningPipeline
from .data_processors.orders_table_cleaner import OrdersCleaner

logger = get_logger(__name__)

class DataCleaningFactory:
    """Factory for creating cleaners."""

    cleaner_map: Dict[str, Type[BaseDataCleaner]] = {
        "orders": OrdersCleaner,
        "geolocation": PassCleaningPipeline
    }

    @classmethod
    def create_cleaner(cls, table_name: str, dataframe: pd.DataFrame) -> BaseDataCleaner:
        """Create appropriate cleaner for the table."""

        cleaner_class = cls.cleaner_map.get(table_name.lower())
        if not cleaner_class:
            raise ValueError(f"No cleaner available for table: {table_name}")

        return cleaner_class(dataframe)


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
    # Example: Run the data cleaning pipeline
    raw_dataframes = ext.DataExtractor(
            source='CSV',
            file_paths={
                'orders': config['RAW_DATA_DIR'] + config['ORDERS_PATH']
            }
    ).extract()

    data_cleaning_pipeline = DataCleaningPipeline(dataframes=raw_dataframes)
    # data_cleaning_pipeline.run()
    print(data_cleaning_pipeline.run())