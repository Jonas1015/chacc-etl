# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0


import luigi
import logging
import time
from config import LOG_LEVEL, LOG_PATH, MAX_RETRIES, RETRY_DELAY

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

from services.progress_service import update_task_progress

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

    def check_interruption(self):
        """Check if pipeline interruption has been requested."""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT result FROM chacc_pipeline_history
                    WHERE status = 'interrupting'
                    AND JSON_EXTRACT(result, '$.interruption_requested') = true
                    LIMIT 1
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    import json
                    data = json.loads(result[0])
                    if data.get('interruption_requested'):
                        return True, data.get('reason', 'Pipeline interrupted by user')
        except Exception as e:
            # Log but don't fail the check
            self.logger.warning(f"Failed to check interruption status: {e}")
        return False, None

    def run(self):
        """Override run method to add progress tracking and interruption checking."""
        task_start_time = time.time()

        try:
            update_task_progress(self.__class__.__name__, 'running')

            self.log_task_execution(self.__class__.__name__, 'RUNNING', task_start_time)

            self.execute_task()

            task_end_time = time.time()
            duration = round(task_end_time - task_start_time, 2)

            self.log_task_execution(self.__class__.__name__, 'DONE', task_start_time, task_end_time, duration)

            update_task_progress(self.__class__.__name__, 'completed')

        except InterruptedException as e:
            task_end_time = time.time()
            duration = round(task_end_time - task_start_time, 2)
            self.log_task_execution(self.__class__.__name__, 'DISABLED', task_start_time, task_end_time, duration, str(e))

            try:
                with self.get_db_connection() as conn:
                    cursor = conn.cursor()
                    import json
                    cursor.execute("""
                        UPDATE chacc_pipeline_history
                        SET status = 'interrupted',
                            result = JSON_SET(COALESCE(result, '{}'), '$.interruption_acknowledged', true)
                        WHERE status = 'interrupting'
                    """)
                    conn.commit()
            except Exception as update_e:
                self.logger.error(f"Failed to acknowledge interruption: {update_e}")

            raise

        except Exception as e:
            interrupted, reason = self.check_interruption()
            if interrupted:
                task_end_time = time.time()
                duration = round(task_end_time - task_start_time, 2)
                self.log_task_execution(self.__class__.__name__, 'DISABLED', task_start_time, task_end_time, duration, reason)

                self.logger.info(f"Task interrupted: {reason}")
                try:
                    with self.get_db_connection() as conn:
                        cursor = conn.cursor()
                        import json
                        cursor.execute("""
                            UPDATE chacc_pipeline_history
                            SET status = 'interrupted',
                                result = JSON_SET(COALESCE(result, '{}'), '$.interruption_acknowledged', true)
                            WHERE status = 'interrupting'
                        """)
                        conn.commit()
                except Exception as update_e:
                    self.logger.error(f"Failed to acknowledge interruption: {update_e}")
                raise InterruptedException(f"Pipeline interrupted: {reason}")
            else:
                task_end_time = time.time()
                duration = round(task_end_time - task_start_time, 2)
                self.log_task_execution(self.__class__.__name__, 'FAILED', task_start_time, task_end_time, duration, str(e))

                update_task_progress(self.__class__.__name__, 'failed')
                # Don't re-raise the exception - let the task fail but allow other tasks to continue
                self.logger.error(f"Task {self.__class__.__name__} failed but pipeline continues: {str(e)}")

    def execute_task(self):
        """Override this method in subclasses to implement actual task logic."""
        raise NotImplementedError("Subclasses must implement execute_task method")


    def log_task_execution(self, task_name, status, start_time, end_time=None, duration=None, error_message=None, records_processed=0):
        """Log individual task execution to pipeline task history."""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT id FROM chacc_pipeline_history
                    WHERE status IN ('running', 'pending', 'interrupting')
                    ORDER BY created_at DESC LIMIT 1
                """)
                pipeline_result = cursor.fetchone()

                if pipeline_result:
                    pipeline_id = pipeline_result[0]

                    task_type = self.task_config.get('type', 'unknown') if hasattr(self, 'task_config') else 'unknown'

                    cursor.execute("""
                        SELECT id FROM chacc_pipeline_task_history
                        WHERE pipeline_history_id = %s AND task_name = %s
                    """, (pipeline_id, task_name))
                    existing_task = cursor.fetchone()

                    if existing_task:
                        cursor.execute("""
                            UPDATE chacc_pipeline_task_history
                            SET status = %s, end_time = %s, duration_seconds = %s, error_message = %s, records_processed = %s
                            WHERE id = %s
                        """, (
                            status,
                            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)) if end_time else None,
                            duration,
                            error_message[:1000] if error_message else None,
                            records_processed,
                            existing_task[0]
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO chacc_pipeline_task_history
                            (pipeline_history_id, task_name, task_type, status, start_time, end_time, duration_seconds, error_message, records_processed)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            pipeline_id,
                            task_name,
                            task_type,
                            status,
                            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)) if start_time else None,
                            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)) if end_time else None,
                            duration,
                            error_message[:1000] if error_message else None,
                            records_processed
                        ))

                    conn.commit()

        except Exception as e:
            self.logger.warning(f"Failed to log task execution: {e}")


class InterruptedException(Exception):
    """Exception raised when a pipeline is interrupted by user request."""
    pass

