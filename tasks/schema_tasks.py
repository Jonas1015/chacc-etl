import luigi
import os
import pymysql
from datetime import datetime
from tasks.base_tasks import TargetDatabaseTask
from utils import log_task_start, log_task_complete, log_task_error, execute_query
from config import DATA_DIR, TARGET_DB_CONFIG

def read_sql_file(filepath):
    """Read SQL content from a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read().strip()

def update_task_status(conn, task_name, status, error_message=None):
    """Update task status in etl_metadata table."""
    try:
        sql = """
            INSERT INTO etl_metadata (table_name, status, error_message, last_update_timestamp, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                error_message = VALUES(error_message),
                last_update_timestamp = VALUES(last_update_timestamp),
                updated_at = VALUES(updated_at)
        """
        execute_query(conn, sql, (task_name, status, error_message, datetime.now(), datetime.now()))
    except Exception as e:
        # If etl_metadata table doesn't exist yet, just log
        print(f"Could not update task status for {task_name}: {e}")

class BaseSQLTask(TargetDatabaseTask):
    """Base class for SQL execution tasks."""

    def update_status(self, status, error_message=None):
        """Update task execution status."""
        try:
            with self.get_db_connection() as conn:
                update_task_status(conn, self.__class__.__name__, status, error_message)
        except:
            pass  # Ignore if status update fails

    def run_sql_file(self, sql_file_path):
        """Execute SQL from a file."""
        sql = read_sql_file(sql_file_path)
        with self.get_db_connection() as conn:
            execute_query(conn, sql)

class CreateTargetDatabaseTask(BaseSQLTask):
    """
    Create the target analytics database if it doesn't exist.
    """

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'target_database_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            # Read database creation SQL
            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'init', 'create_database.sql')
            create_db_sql = read_sql_file(sql_file)

            # Create database using connection without database specified
            db_config_no_db = TARGET_DB_CONFIG.copy()
            db_config_no_db.pop('database', None)

            conn = pymysql.connect(**db_config_no_db)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_db_sql)
                conn.commit()
            finally:
                conn.close()

            # Mark as complete
            with self.output().open('w') as f:
                f.write('Target database created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateETLMetadataTableTask(BaseSQLTask):
    """Create ETL metadata table."""

    def requires(self):
        return CreateTargetDatabaseTask()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'etl_metadata_table_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables', 'etl_metadata.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('ETL metadata table created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise