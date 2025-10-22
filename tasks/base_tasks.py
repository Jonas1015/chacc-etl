# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0


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

# Import progress service for task lifecycle events
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
                    SELECT result FROM pipeline_history
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
            # Signal task start
            update_task_progress(self.__class__.__name__, 'running')

            # Log task start to pipeline task history
            self.log_task_execution(self.__class__.__name__, 'running', task_start_time)

            # Call the actual task implementation
            self.execute_task()

            # Calculate duration
            task_end_time = time.time()
            duration = round(task_end_time - task_start_time, 2)

            # Log task completion
            self.log_task_execution(self.__class__.__name__, 'completed', task_start_time, task_end_time, duration)

            # Signal task completion
            update_task_progress(self.__class__.__name__, 'completed')

        except InterruptedException as e:
            # Handle interruption
            task_end_time = time.time()
            duration = round(task_end_time - task_start_time, 2)
            self.log_task_execution(self.__class__.__name__, 'interrupted', task_start_time, task_end_time, duration, str(e))

            # Update the interrupting record to interrupted
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

            # Re-raise the interruption
            raise

        except Exception as e:
            # Check if this is an interruption
            interrupted, reason = self.check_interruption()
            if interrupted:
                task_end_time = time.time()
                duration = round(task_end_time - task_start_time, 2)
                self.log_task_execution(self.__class__.__name__, 'interrupted', task_start_time, task_end_time, duration, reason)

                self.logger.info(f"Task interrupted: {reason}")
                # Update the interrupting record to interrupted
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
                # Re-raise as a specific interruption exception
                raise InterruptedException(f"Pipeline interrupted: {reason}")
            else:
                # Log task failure
                task_end_time = time.time()
                duration = round(task_end_time - task_start_time, 2)
                self.log_task_execution(self.__class__.__name__, 'failed', task_start_time, task_end_time, duration, str(e))

                # Signal task failure
                update_task_progress(self.__class__.__name__, 'failed')
                raise

    def execute_task(self):
        """Override this method in subclasses to implement actual task logic."""
        raise NotImplementedError("Subclasses must implement execute_task method")


    def log_task_execution(self, task_name, status, start_time, end_time=None, duration=None, error_message=None, records_processed=0):
        """Log individual task execution to pipeline task history."""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()

                # Get current pipeline execution ID
                cursor.execute("""
                    SELECT id FROM chacc_pipeline_history
                    WHERE status IN ('running', 'pending', 'interrupting')
                    ORDER BY created_at DESC LIMIT 1
                """)
                pipeline_result = cursor.fetchone()

                if pipeline_result:
                    pipeline_id = pipeline_result[0]

                    # Determine task type from class hierarchy
                    task_type = 'unknown'
                    if hasattr(self, '__class__'):
                        class_name = self.__class__.__name__
                        # Check task type based on naming patterns or inheritance
                        if 'ExtractLoad' in class_name or 'extract_load' in str(self.task_config.get('type', '')):
                            task_type = 'extract_load'
                        elif 'Procedure' in class_name or 'procedure' in str(self.task_config.get('type', '')):
                            task_type = 'procedure'
                        elif 'Schema' in class_name or 'schema' in str(self.task_config.get('type', '')):
                            task_type = 'schema'
                        elif 'Flattened' in class_name or 'flattened' in str(self.task_config.get('type', '')):
                            task_type = 'flattened'
                        elif 'Summary' in class_name or 'summary' in str(self.task_config.get('type', '')):
                            task_type = 'summary'

                    cursor.execute("""
                        INSERT INTO pipeline_task_history
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
                        error_message[:1000] if error_message else None,  # Truncate long error messages
                        records_processed
                    ))

                    conn.commit()

        except Exception as e:
            # Don't fail the task if logging fails, just log the error
            self.logger.warning(f"Failed to log task execution: {e}")


class InterruptedException(Exception):
    """Exception raised when a pipeline is interrupted by user request."""
    pass

