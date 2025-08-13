# data_processors/orders_table_cleaner.py
"""Cleaner specific for orders table."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from config.config import config
from config.log_config import get_logger
from .base_cleaner import BaseDataCleaner, data_type_mapping

logger = get_logger(__name__)

class OrdersCleaner(BaseDataCleaner):
    """Orders-specific cleaning logic."""

    def __init__(self, raw_data: pd.DataFrame, table_name: str = config['ORDERS_TABLE']):
        super().__init__(raw_data, table_name)

    def _validate_order_status_dates(self):
        """Validate order status consistency."""
        # Delivered orders should have delivery dates
        df = self.cleaned_data.copy()

        problematic_mask = (df['order_status'] == 'delivered') \
                & (df['order_approved_at'].isna() \
                | df['order_delivered_carrier_date'].isna() \
                | df['order_delivered_customer_date'].isna())

        df.drop(df.loc[problematic_mask].index, inplace=True)
        self.cleaned_data = df.copy()
        return self

    def _validate_timestamps_business_logic(self):
        """Validate timestamps follow business logic.
            order_purchase_timestamp
                <= order_approved_at
                    <= order_delivered_carrier_date
                        <= order_delivered_customer_date
        """

        df = self.cleaned_data.copy()

        # conditions for timestamp validation
        conditions = []

        # purchase <= approved
        mask1 = (df['order_purchase_timestamp'].notna() & df['order_approved_at'].notna())
        cond1 = df['order_purchase_timestamp'] <= df['order_approved_at']
        conditions.append(~mask1 | cond1)

        # approved <= delivered to carrier
        mask2 = (df['order_approved_at'].notna() & df['order_delivered_carrier_date'].notna())
        cond2 = df['order_approved_at'] <= df['order_delivered_carrier_date']
        conditions.append(~mask2 | cond2)

        # delivered to carrier <= delivered to customer
        mask3 = (df['order_delivered_carrier_date'].notna() & df['order_delivered_customer_date'].notna())
        cond3 = df['order_delivered_carrier_date'] <= df['order_delivered_customer_date']
        conditions.append(~mask3 | cond3)

        # all conditions must be true for valid rows
        valid_rows = conditions[0] & conditions[1] & conditions[2]

        # keeping only valid rows
        self.cleaned_data = df.loc[valid_rows].copy()

        return self

    def clean(self) -> pd.DataFrame:
        """Main cleaning pipeline for orders table."""
        logger.info("Starting orders cleaning process")

        try:
            # Execute cleaning pipeline step by step
            self.cleaned_data = self.raw_data.copy()
            self.cleaned_data = self.cleaned_data.drop_duplicates(subset=['order_id'], keep='first')

            (self
                .data_type_validation(data_type_mapping.get(self.table_name))
                ._validate_order_status_dates()
                ._validate_timestamps_business_logic())

            logger.info("Orders cleaning process completed")
            return self.cleaned_data

        except Exception as e:
            logger.error(f"Error during orders cleaning: {str(e)}")
            raise



if __name__ == "__main__":
    raw_orders_data = pd.read_csv(config['RAW_DATA_DIR'] + config['ORDERS_PATH'])
    cleaned_orders = OrdersCleaner(raw_orders_data).clean()
    print(cleaned_orders.shape)
    print(cleaned_orders.dtypes)

