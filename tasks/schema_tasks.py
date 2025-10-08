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

class CreatePatientsTableTask(BaseSQLTask):
    """Create patients table."""

    def requires(self):
        return CreateTargetDatabaseTask()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'patients_table_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables', 'patients.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Patients table created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateEncountersTableTask(BaseSQLTask):
    """Create encounters table."""

    def requires(self):
        return CreateTargetDatabaseTask()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'encounters_table_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables', 'encounters.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Encounters table created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateObservationsTableTask(BaseSQLTask):
    """Create observations table."""

    def requires(self):
        return CreateTargetDatabaseTask()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'observations_table_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables', 'observations.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Observations table created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateLocationsTableTask(BaseSQLTask):
    """Create locations table."""

    def requires(self):
        return CreateTargetDatabaseTask()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'locations_table_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables', 'locations.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Locations table created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateFlattenedPatientEncountersProcedureTask(BaseSQLTask):
    """Create stored procedure for flattened patient encounters."""

    def requires(self):
        return [CreatePatientsTableTask(), CreateEncountersTableTask()]

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'flattened_patient_encounters_procedure_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'procedures', 'create_flattened_patient_encounters.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Flattened patient encounters procedure created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateFlattenedObservationsProcedureTask(BaseSQLTask):
    """Create stored procedure for flattened observations."""

    def requires(self):
        return [CreatePatientsTableTask(), CreateEncountersTableTask(), CreateObservationsTableTask(), CreateLocationsTableTask()]

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'flattened_observations_procedure_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'procedures', 'create_flattened_observations.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Flattened observations procedure created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateIncrementalPatientUpdateProcedureTask(BaseSQLTask):
    """Create stored procedure for incremental patient updates."""

    def requires(self):
        return [CreatePatientsTableTask()]

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'incremental_patient_update_procedure_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'procedures', 'incremental_patient_update.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Incremental patient update procedure created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateIncrementalEncounterUpdateProcedureTask(BaseSQLTask):
    """Create stored procedure for incremental encounter updates."""

    def requires(self):
        return [CreatePatientsTableTask(), CreateEncountersTableTask()]

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'incremental_encounter_update_procedure_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'procedures', 'incremental_encounter_update.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Incremental encounter update procedure created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise

class CreateIncrementalObservationUpdateProcedureTask(BaseSQLTask):
    """Create stored procedure for incremental observation updates."""

    def requires(self):
        return [CreatePatientsTableTask(), CreateEncountersTableTask(), CreateObservationsTableTask(), CreateLocationsTableTask()]

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'incremental_observation_update_procedure_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'procedures', 'incremental_observation_update.sql')
            self.run_sql_file(sql_file)

            with self.output().open('w') as f:
                f.write('Incremental observation update procedure created successfully\n')

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

class CreateETLWatermarksTableTask(BaseSQLTask):
    """Create ETL watermarks table."""

    def requires(self):
        return CreateTargetDatabaseTask()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'etl_watermarks_table_created.txt'))

    def run(self):
        try:
            log_task_start(self)
            self.update_status('running')

            sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables', 'etl_watermarks.sql')
            self.run_sql_file(sql_file)

            # Initialize watermark records
            init_watermarks = """
                INSERT IGNORE INTO etl_watermarks (source_table, target_table, batch_size) VALUES
                ('patient', 'patients', 1000),
                ('encounter', 'encounters', 1000),
                ('obs', 'observations', 5000),
                ('location', 'locations', 100)
            """
            with self.get_db_connection() as conn:
                execute_query(conn, init_watermarks)

            with self.output().open('w') as f:
                f.write('ETL watermarks table created successfully\n')

            self.update_status('completed')
            log_task_complete(self)
        except Exception as e:
            self.update_status('failed', str(e))
            log_task_error(self, e)
            raise