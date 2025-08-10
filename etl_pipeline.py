import os
from dotenv import load_dotenv

from config.log_config import get_logger
from pipeline.data_cleaning import DataCleaningPipeline  # TRANSFORM
from pipeline.extractor import DataExtractor  # EXTRACT
from pipeline.loader import DataLoader  # LOAD

logger = get_logger(__name__)
load_dotenv()


class ETLPipeline:
    """ETL Pipeline for data processing.

    input kwargs must be like:
    extractor_pipeline_args = {
        'extractor_source': 'source_str',
        'file_paths': {'customers': 'customers.csv'}
    }
    """

    def __init__(self, **kwargs):
        if kwargs.get("extractor_pipeline_args") is None \
                or kwargs.get("loading_pipeline_args") is None:
            raise ValueError("Missing pipeline arguments")

        self.extractor_settings = kwargs.get("extractor_pipeline_args", {})
        self.cleaning_settings = kwargs.get("cleaning_pipeline_args", {})
        self.loading_settings = kwargs.get("loading_pipeline_args", {})



    def run(self):
        """Run the complete ETL pipeline."""
        try:
            logger.info("🚀 Starting ETL pipeline")

            ext = DataExtractor(
                source=self.extractor_settings.get('extractor_source'),
                file_paths=self.extractor_settings.get('file_paths')
            ).extract()

            logger.info("📥 Data extraction completed and 🔄 Starting data transformation")
            # transformer = DataCleaningPipeline(ext).run()

            logger.info("🧹 Data transformation completed and 💾 Starting data loading")
            DataLoader(
                source=self.loading_settings.get('source'),
                dataframe_table_mapping=ext,
                schema=self.loading_settings.get('schema')
            ).load_data()
            logger.info("✅ Data loading completed successfully")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise



# import schedule
# import time
# import snowflake.connector
# from datetime import datetime

# class DataPipelineManager:
#     def __init__(self):
#         self.conn = snowflake.connector.connect(
#             account='your-account',
#             user='your-user',
#             password='your-password',
#             warehouse='COMPUTE_WH',
#             database='OLIST_DB'
#         )

#     def refresh_staging(self):
#         print(f"🔄 Starting staging refresh at {datetime.now()}")
#         cursor = self.conn.cursor()

#         staging_queries = [
#             """CREATE OR REPLACE VIEW staging.customers AS
#                SELECT customer_id, UPPER(TRIM(customer_city))... FROM raw.customers""",
#             """CREATE OR REPLACE VIEW staging.orders AS
#                SELECT order_id, TO_TIMESTAMP(order_purchase_timestamp)... FROM raw.orders"""
#         ]

#         for query in staging_queries:
#             cursor.execute(query)

#         print("✅ Staging refreshed!")

#     def refresh_transformed(self):
#         print(f"🔄 Starting transformed refresh at {datetime.now()}")
#         cursor = self.conn.cursor()

#         # Incremental approach
#         cursor.execute("""
#             INSERT INTO transformed.order_metrics
#             SELECT o.order_id, c.region, o.delivery_delay_days...
#             FROM staging.orders o
#             LEFT JOIN staging.customers c ON o.customer_id = c.customer_id
#             WHERE o.order_purchase_timestamp > (
#                 SELECT COALESCE(MAX(order_date), '2000-01-01')
#                 FROM transformed.order_metrics
#             )
#         """)

#         print("✅ Transformed refreshed!")

# # Setup scheduler
# pipeline = DataPipelineManager()

# # هر 2 ساعت staging را refresh کن
# schedule.every(2).hours.do(pipeline.refresh_staging)

# # هر 4 ساعت transformed را refresh کن
# schedule.every(4).hours.do(pipeline.refresh_transformed)

# # اجرا
# while True:
#     schedule.run_pending()
#     time.sleep(60)  # چک کردن هر دقیقه



# # 💡 Usage Example for Olist:
# if __name__ == "__main__":
#     # Example: Load multiple Olist tables
#     df_mapping = {
#         customers_df: 'customers',
#         orders_df: 'orders',
#         order_items_df: 'order_items',
#         reviews_df: 'reviews'
#     }

#     loader = DataLoader(
#         source='Snowflake',
#         dataframe_table_mapping=df_mapping,
#         schema='raw',
#         database='olist_analytics'
#     )

#     loader.load_data()

# # ---- ---- -- - - --
# """Main ETL Pipeline - Extract, Clean, Load."""

# import os
# import sys
# from pathlib import Path

# # Add project root to path
# project_root = Path(__file__).parent.parent
# sys.path.append(str(project_root))

# from config.log_config import logger
# from pipeline.data_cleaning import DataCleaningPipeline
# from pipeline.extractor import DataExtractor
# from pipeline.loader import DataLoader


# def main():
#     """Run complete ETL pipeline."""
#     logger.info("🚀 Starting complete ETL pipeline")

#     # ===== STEP 1: EXTRACT DATA =====
#     logger.info("📥 Step 1: Extracting raw data from CSV files")

#     # Define all CSV file paths
#     csv_files = [
#         'data/raw/olist_orders_dataset.csv',
#         'data/raw/olist_products_dataset.csv',
#         'data/raw/olist_customers_dataset.csv',
#         'data/raw/olist_order_items_dataset.csv',
#         'data/raw/olist_order_payments_dataset.csv',
#         'data/raw/olist_order_reviews_dataset.csv',
#         'data/raw/olist_sellers_dataset.csv',
#         'data/raw/olist_geolocation_dataset.csv',
#         'data/raw/product_category_name_translation.csv'
#     ]

#     try:
#         # Extract data
#         extractor = DataExtractor('CSV', file_paths=csv_files)
#         raw_dataframes_list = extractor.extract()

#         # Convert list to dictionary with file names as keys
#         raw_dataframes = {}
#         for i, df in enumerate(raw_dataframes_list):
#             file_name = Path(csv_files[i]).stem  # Get filename without extension
#             raw_dataframes[file_name] = df

#         logger.info(f"✅ Successfully extracted {len(raw_dataframes)} tables")

#         # Log basic info about each table
#         for name, df in raw_dataframes.items():
#             logger.info(f"  - {name}: {df.shape[0]} rows, {df.shape[1]} columns")

#     except Exception as e:
#         logger.error(f"❌ Data extraction failed: {str(e)}")
#         return

#     # ===== STEP 2: CLEAN DATA =====
#     logger.info("🧹 Step 2: Cleaning data with business rules")

#     try:
#         # Clean all tables
#         cleaning_pipeline = DataCleaningPipeline()
#         cleaned_dataframes = cleaning_pipeline.clean_all_tables(raw_dataframes)

#         logger.info("✅ Data cleaning completed")

#         # Generate and save cleaning report
#         cleaning_summary = cleaning_pipeline.get_pipeline_summary()
#         cleaning_summary.to_csv('data/processed/cleaning_report.csv', index=False)
#         logger.info("📊 Cleaning report saved to data/processed/cleaning_report.csv")

#         # Display summary
#         print("\n" + "="*60)
#         print("📊 DATA CLEANING SUMMARY")
#         print("="*60)
#         print(cleaning_summary.to_string(index=False))
#         print("="*60)

#     except Exception as e:
#         logger.error(f"❌ Data cleaning failed: {str(e)}")
#         return

#     # ===== STEP 3: SAVE PROCESSED DATA =====
#     logger.info("💾 Step 3: Saving cleaned data")

#     try:
#         # Save cleaned dataframes to CSV
#         os.makedirs('data/processed', exist_ok=True)

#         for table_name, df in cleaned_dataframes.items():
#             output_path = f'data/processed/{table_name}_cleaned.csv'
#             df.to_csv(output_path, index=False)
#             logger.info(f"  - Saved {table_name}: {output_path}")

#         logger.info("✅ All cleaned data saved successfully")

#     except Exception as e:
#         logger.error(f"❌ Saving processed data failed: {str(e)}")
#         return

#     # ===== STEP 4: LOAD TO DATABASE (OPTIONAL) =====
#     logger.info("🗄️ Step 4: Loading to database (if configured)")

#     try:
#         # Check if database loading is configured
#         # You can add your DataLoader logic here
#         # loader = DataLoader(target='snowflake')  # or postgres
#         # loader.load_multiple_tables(cleaned_dataframes)

#         logger.info("ℹ️ Database loading skipped (configure DataLoader if needed)")

#     except Exception as e:
#         logger.error(f"❌ Database loading failed: {str(e)}")

#     # ===== PIPELINE COMPLETED =====
#     logger.info("🎉 ETL Pipeline completed successfully!")
#     print("\n" + "🎉 ETL PIPELINE COMPLETED!" + "\n")
#     print(f"✅ Extracted: {len(raw_dataframes)} tables")
#     print(f"✅ Cleaned: {len(cleaned_dataframes)} tables")
#     print(f"✅ Saved to: data/processed/")
#     print(f"📊 Report: data/processed/cleaning_report.csv")

# if __name__ == "__main__":
#     main()