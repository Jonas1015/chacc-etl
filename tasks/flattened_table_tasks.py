import luigi
import os
import json
from tasks.base_tasks import TargetDatabaseTask
from utils import log_task_start, log_task_complete, log_task_error, execute_query
from config import DATA_DIR

# Load task dependencies configuration
def load_task_dependencies():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'task_dependencies.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get('task_dependencies', {})
    return {}

_task_dependencies = load_task_dependencies()

class CreateFlattenedPatientEncountersTask(TargetDatabaseTask):
    """
    Create flattened patient encounters table using stored procedure.
    """

    def requires(self):
        deps = _task_dependencies.get('CreateFlattenedPatientEncountersTask', [])
        # Import required modules dynamically
        from tasks import load_tasks, schema_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(load_tasks, dep):
                required_tasks.append(getattr(load_tasks, dep)())
            elif hasattr(schema_tasks, dep):
                required_tasks.append(getattr(schema_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'flattened_patient_encounters_created.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Call stored procedure to create flattened table
            with self.get_db_connection() as conn:
                execute_query(conn, "CALL sp_create_flattened_patient_encounters()")

            # Mark as complete
            with self.output().open('w') as f:
                f.write('Flattened patient encounters table created successfully\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class CreateFlattenedObservationsTask(TargetDatabaseTask):
    """
    Create flattened observations table using stored procedure.
    """

    def requires(self):
        deps = _task_dependencies.get('CreateFlattenedObservationsTask', [])
        # Import required modules dynamically
        from tasks import load_tasks, schema_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(load_tasks, dep):
                required_tasks.append(getattr(load_tasks, dep)())
            elif hasattr(schema_tasks, dep):
                required_tasks.append(getattr(schema_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'flattened_observations_created.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Call stored procedure to create flattened table
            with self.get_db_connection() as conn:
                execute_query(conn, "CALL sp_create_flattened_observations()")

            # Mark as complete
            with self.output().open('w') as f:
                f.write('Flattened observations table created successfully\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class CreatePatientSummaryTableTask(TargetDatabaseTask):
    """
    Create patient summary table with aggregated statistics.
    """

    def requires(self):
        deps = _task_dependencies.get('CreatePatientSummaryTableTask', [])
        # Import required modules dynamically
        from tasks import flattened_table_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(flattened_table_tasks, dep):
                required_tasks.append(getattr(flattened_table_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'patient_summary_created.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Create patient summary table
            create_query = """
                CREATE TABLE IF NOT EXISTS patient_summary AS
                SELECT
                    p.patient_id,
                    p.patient_identifier,
                    p.openmrs_id,
                    CONCAT_WS(' ', p.given_name, p.middle_name, p.family_name) as full_name,
                    p.birthdate,
                    TIMESTAMPDIFF(YEAR, p.birthdate, CURDATE()) as age_years,
                    p.gender,
                    p.dead,
                    p.death_date,
                    COUNT(DISTINCT e.encounter_id) as total_encounters,
                    COUNT(DISTINCT DATE(e.encounter_datetime)) as total_visit_days,
                    MIN(e.encounter_datetime) as first_encounter_date,
                    MAX(e.encounter_datetime) as last_encounter_date,
                    DATEDIFF(MAX(e.encounter_datetime), MIN(e.encounter_datetime)) as days_between_first_last_visit,
                    AVG(TIMESTAMPDIFF(DAY, LAG(e.encounter_datetime) OVER (PARTITION BY p.patient_id ORDER BY e.encounter_datetime), e.encounter_datetime)) as avg_days_between_visits,
                    p.date_created as patient_registration_date,
                    p.date_changed as patient_last_updated
                FROM patients p
                LEFT JOIN flattened_patient_encounters e ON p.patient_id = e.patient_id
                GROUP BY p.patient_id, p.patient_identifier, p.openmrs_id, p.given_name, p.middle_name,
                         p.family_name, p.birthdate, p.gender, p.dead, p.death_date,
                         p.date_created, p.date_changed
            """

            with self.get_db_connection() as conn:
                execute_query(conn, create_query)

                # Create indexes (ignore errors if they already exist)
                index_queries = [
                    "CREATE INDEX idx_ps_patient_id ON patient_summary (patient_id)",
                    "CREATE INDEX idx_ps_gender ON patient_summary (gender)",
                    "CREATE INDEX idx_ps_age ON patient_summary (age_years)",
                    "CREATE INDEX idx_ps_registration ON patient_summary (patient_registration_date)",
                    "CREATE INDEX idx_ps_last_visit ON patient_summary (last_encounter_date)"
                ]

                for query in index_queries:
                    try:
                        execute_query(conn, query)
                    except Exception as e:
                        # Ignore index creation errors (index might already exist)
                        if "Duplicate key name" not in str(e):
                            raise

            # Mark as complete
            with self.output().open('w') as f:
                f.write('Patient summary table created successfully\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class CreateObservationSummaryTableTask(TargetDatabaseTask):
    """
    Create observation summary table with aggregated statistics by concept.
    """

    def requires(self):
        deps = _task_dependencies.get('CreateObservationSummaryTableTask', [])
        # Import required modules dynamically
        from tasks import flattened_table_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(flattened_table_tasks, dep):
                required_tasks.append(getattr(flattened_table_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'observation_summary_created.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Create observation summary table
            create_query = """
                CREATE TABLE IF NOT EXISTS observation_summary AS
                SELECT
                    concept_id,
                    concept_name,
                    datatype_name,
                    COUNT(*) as total_observations,
                    COUNT(DISTINCT patient_id) as unique_patients,
                    COUNT(DISTINCT encounter_id) as unique_encounters,
                    MIN(obs_datetime) as first_observation_date,
                    MAX(obs_datetime) as last_observation_date,
                    COUNT(CASE WHEN actual_value IS NOT NULL AND actual_value != '' THEN 1 END) as non_null_values,
                    AVG(CASE WHEN actual_value REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(actual_value AS DECIMAL(20,2)) ELSE NULL END) as avg_numeric_value,
                    MIN(CASE WHEN actual_value REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(actual_value AS DECIMAL(20,2)) ELSE NULL END) as min_numeric_value,
                    MAX(CASE WHEN actual_value REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(actual_value AS DECIMAL(20,2)) ELSE NULL END) as max_numeric_value,
                    GROUP_CONCAT(DISTINCT actual_value ORDER BY actual_value SEPARATOR '|') as distinct_values_sample
                FROM flattened_observations
                GROUP BY concept_id, concept_name, datatype_name
                HAVING total_observations > 0
                ORDER BY total_observations DESC
            """

            with self.get_db_connection() as conn:
                execute_query(conn, create_query)

                # Create indexes (ignore errors if they already exist)
                index_queries = [
                    "CREATE INDEX idx_os_concept_id ON observation_summary (concept_id)",
                    "CREATE INDEX idx_os_total_obs ON observation_summary (total_observations)",
                    "CREATE INDEX idx_os_unique_patients ON observation_summary (unique_patients)"
                ]

                for query in index_queries:
                    try:
                        execute_query(conn, query)
                    except Exception as e:
                        # Ignore index creation errors (index might already exist)
                        if "Duplicate key name" not in str(e):
                            raise

            # Mark as complete
            with self.output().open('w') as f:
                f.write('Observation summary table created successfully\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class CreateLocationSummaryTableTask(TargetDatabaseTask):
    """
    Create location/facility summary table.
    """

    def requires(self):
        deps = _task_dependencies.get('CreateLocationSummaryTableTask', [])
        # Import required modules dynamically
        from tasks import load_tasks, flattened_table_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(load_tasks, dep):
                required_tasks.append(getattr(load_tasks, dep)())
            elif hasattr(flattened_table_tasks, dep):
                required_tasks.append(getattr(flattened_table_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'location_summary_created.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Create location summary table
            create_query = """
                CREATE TABLE IF NOT EXISTS location_summary AS
                SELECT
                    l.location_id,
                    l.name as location_name,
                    l.location_type_name,
                    l.city_village,
                    l.state_province,
                    l.country,
                    COUNT(DISTINCT e.patient_id) as unique_patients_served,
                    COUNT(DISTINCT e.encounter_id) as total_encounters,
                    COUNT(DISTINCT DATE(e.encounter_datetime)) as active_days,
                    MIN(e.encounter_datetime) as first_encounter_date,
                    MAX(e.encounter_datetime) as last_encounter_date,
                    NULL as avg_days_between_encounters,
                    l.creator_name as location_creator,
                    l.date_created as location_created_date
                FROM locations l
                LEFT JOIN flattened_patient_encounters e ON l.location_id = e.location_id
                GROUP BY l.location_id, l.name, l.location_type_name, l.city_village,
                         l.state_province, l.country, l.creator_name, l.date_created
            """

            with self.get_db_connection() as conn:
                execute_query(conn, create_query)

                # Create indexes (ignore errors if they already exist)
                index_queries = [
                    "CREATE INDEX idx_ls_location_id ON location_summary (location_id)",
                    "CREATE INDEX idx_ls_type ON location_summary (location_type_name)",
                    "CREATE INDEX idx_ls_patients ON location_summary (unique_patients_served)",
                    "CREATE INDEX idx_ls_city ON location_summary (city_village)"
                ]

                for query in index_queries:
                    try:
                        execute_query(conn, query)
                    except Exception as e:
                        # Ignore index creation errors (index might already exist)
                        if "Duplicate key name" not in str(e):
                            raise

            # Mark as complete
            with self.output().open('w') as f:
                f.write('Location summary table created successfully\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise