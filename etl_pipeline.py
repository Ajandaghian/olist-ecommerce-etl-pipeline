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
            transformer = DataCleaningPipeline(ext).run()

            logger.info("🧹 Data transformation completed and 💾 Starting data loading")
            DataLoader(
                source=self.loading_settings.get('source'),
                dataframe_table_mapping=transformer,
                schema=self.loading_settings.get('schema')
            ).load_data()
            logger.info("✅ Data loading completed successfully")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
