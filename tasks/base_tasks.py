import luigi
import logging
from config import LOG_LEVEL, LOG_PATH, MAX_RETRIES, RETRY_DELAY

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

class BaseETLTask(luigi.Task):
    """
    Base class for all ETL tasks with common functionality.
    """
    date = luigi.DateParameter(default=None)
    retry_count = luigi.IntParameter(default=0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def max_retries(self):
        return MAX_RETRIES

    @property
    def retry_delay(self):
        return RETRY_DELAY

    def on_failure(self, exception):
        self.logger.error(f"Task {self.task_id} failed: {exception}")
        if self.retry_count < self.max_retries:
            self.logger.info(f"Retrying task {self.task_id} in {self.retry_delay} seconds")
            import time
            time.sleep(self.retry_delay)
            return self.clone(retry_count=self.retry_count + 1)
        return super().on_failure(exception)

class SourceDatabaseTask(BaseETLTask):
    """
    Base class for tasks that interact with the source database.
    """
    def get_db_connection(self):
        """Get a source database connection context manager."""
        from utils.db_utils import get_source_db_connection
        return get_source_db_connection()

class TargetDatabaseTask(BaseETLTask):
    """
    Base class for tasks that interact with the target database.
    """
    def get_db_connection(self):
        """Get a target database connection context manager."""
        from utils.db_utils import get_target_db_connection
        return get_target_db_connection()

