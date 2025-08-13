from etl_pipeline import ETLPipeline
import schedule
import time

class ETL:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def run(self):
        ETLPipeline(**self.kwargs).run()

    def scheduled_run(self, interval):
        """Run the ETL pipeline on a schedule."""

        schedule.every(interval).seconds.do(self.run)
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopped by user.")