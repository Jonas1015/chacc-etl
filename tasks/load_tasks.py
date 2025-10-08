import luigi
import json
import os
from datetime import datetime
from tasks.base_tasks import TargetDatabaseTask
from tasks.extract_tasks import ExtractPatientsTask, ExtractEncountersTask, ExtractObservationsTask, ExtractLocationsTask
from tasks.schema_tasks import CreateETLMetadataTableTask
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

class LoadPatientsTask(TargetDatabaseTask):
    """
    Load patient data into target database using efficient bulk insert.
    """
    incremental = luigi.BoolParameter(default=False)
    last_updated = luigi.DateParameter(default=None)

    def requires(self):
        deps = _task_dependencies.get('LoadPatientsTask', [])
        # Import required modules dynamically
        from tasks import schema_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(schema_tasks, dep):
                required_tasks.append(getattr(schema_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'patients_loaded.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Read extracted data
            with self.input()[1].open('r') as f:
                patients = json.load(f)

            if not patients:
                self.logger.info("No patient data to load")
                with self.output().open('w') as f:
                    f.write('No patients to load\n')
                return

            # Prepare data for insertion
            data = [(
                p['patient_id'], p['patient_identifier'], p['openmrs_id'],
                p['given_name'], p['middle_name'], p['family_name'],
                p['birthdate'], p['birthdate_estimated'], p['gender'],
                p['dead'], p['death_date'], p['date_created'], p['date_changed'], p['uuid']
            ) for p in patients]

            # Use efficient bulk insert
            if self.incremental:
                # For incremental loads, use ON DUPLICATE KEY UPDATE
                insert_query = """
                    INSERT INTO patients (
                        patient_id, patient_identifier, openmrs_id, given_name, middle_name, family_name,
                        birthdate, birthdate_estimated, gender, dead, death_date, date_created, date_changed, uuid
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        openmrs_id = VALUES(openmrs_id),
                        given_name = VALUES(given_name),
                        middle_name = VALUES(middle_name),
                        family_name = VALUES(family_name),
                        birthdate = VALUES(birthdate),
                        birthdate_estimated = VALUES(birthdate_estimated),
                        gender = VALUES(gender),
                        dead = VALUES(dead),
                        death_date = VALUES(death_date),
                        date_changed = VALUES(date_changed)
                """
            else:
                # For full loads, truncate first
                with self.get_db_connection() as conn:
                    execute_query(conn, "TRUNCATE TABLE patients")
                insert_query = """
                    INSERT INTO patients (
                        patient_id, patient_identifier, openmrs_id, given_name, middle_name, family_name,
                        birthdate, birthdate_estimated, gender, dead, death_date, date_created, date_changed, uuid
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            # Execute bulk insert
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, data)
                    conn.commit()

            # Update metadata
            self._update_metadata('patients', len(patients))

            # Mark as complete
            with self.output().open('w') as f:
                f.write(f'Patients loaded successfully: {len(patients)} records\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

    def _update_metadata(self, table_name, record_count):
        """Update ETL metadata table."""
        query = """
            INSERT INTO etl_metadata (table_name, record_count, status, updated_at)
            VALUES (%s, %s, 'completed', NOW())
            ON DUPLICATE KEY UPDATE
                record_count = VALUES(record_count),
                status = 'completed',
                updated_at = NOW()
        """
        with self.get_db_connection() as conn:
            execute_query(conn, query, (table_name, record_count))

class LoadEncountersTask(TargetDatabaseTask):
    """
    Load encounter data into target database.
    """
    incremental = luigi.BoolParameter(default=False)
    last_updated = luigi.DateParameter(default=None)

    def requires(self):
        deps = _task_dependencies.get('LoadEncountersTask', [])
        # Import required modules dynamically
        from tasks import schema_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(schema_tasks, dep):
                required_tasks.append(getattr(schema_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'encounters_loaded.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Read extracted data
            with self.input()[1].open('r') as f:
                encounters = json.load(f)

            if not encounters:
                self.logger.info("No encounter data to load")
                with self.output().open('w') as f:
                    f.write('No encounters to load\n')
                return

            # Prepare data for insertion
            data = [(
                e['encounter_id'], e['patient_id'], e['encounter_type'], e['encounter_type_name'],
                e['form_id'], e['form_name'], e['form_uuid'], e['encounter_datetime'],
                e['provider_id'], e['provider_name'], e['location_id'], e['location_name'],
                e['date_created'], e['date_changed'], e['uuid']
            ) for e in encounters]

            # Use efficient bulk insert
            if self.incremental:
                insert_query = """
                    INSERT INTO encounters (
                        encounter_id, patient_id, encounter_type, encounter_type_name,
                        form_id, form_name, form_uuid, encounter_datetime,
                        provider_id, provider_name, location_id, location_name,
                        date_created, date_changed, uuid
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        encounter_datetime = VALUES(encounter_datetime),
                        provider_id = VALUES(provider_id),
                        provider_name = VALUES(provider_name),
                        location_id = VALUES(location_id),
                        location_name = VALUES(location_name),
                        date_changed = VALUES(date_changed)
                """
            else:
                with self.get_db_connection() as conn:
                    execute_query(conn, "TRUNCATE TABLE encounters")
                insert_query = """
                    INSERT INTO encounters (
                        encounter_id, patient_id, encounter_type, encounter_type_name,
                        form_id, form_name, form_uuid, encounter_datetime,
                        provider_id, provider_name, location_id, location_name,
                        date_created, date_changed, uuid
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            # Execute bulk insert
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, data)
                    conn.commit()

            # Update metadata
            self._update_metadata('encounters', len(encounters))

            # Mark as complete
            with self.output().open('w') as f:
                f.write(f'Encounters loaded successfully: {len(encounters)} records\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

    def _update_metadata(self, table_name, record_count):
        """Update ETL metadata table."""
        query = """
            INSERT INTO etl_metadata (table_name, record_count, status, updated_at)
            VALUES (%s, %s, 'completed', NOW())
            ON DUPLICATE KEY UPDATE
                record_count = VALUES(record_count),
                status = 'completed',
                updated_at = NOW()
        """
        with self.get_db_connection() as conn:
            execute_query(conn, query, (table_name, record_count))

class LoadObservationsTask(TargetDatabaseTask):
    """
    Load observation data into target database.
    """
    incremental = luigi.BoolParameter(default=False)
    last_updated = luigi.DateParameter(default=None)

    def requires(self):
        deps = _task_dependencies.get('LoadObservationsTask', [])
        # Import required modules dynamically
        from tasks import schema_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(schema_tasks, dep):
                required_tasks.append(getattr(schema_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'observations_loaded.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Read extracted data
            with self.input()[1].open('r') as f:
                observations = json.load(f)

            if not observations:
                self.logger.info("No observation data to load")
                with self.output().open('w') as f:
                    f.write('No observations to load\n')
                return

            # Prepare data for insertion
            data = []
            for obs in observations:
                # Determine actual value
                actual_value = None
                if obs['value_coded'] is not None:
                    actual_value = str(obs['value_coded'])
                elif obs['value_numeric'] is not None:
                    actual_value = str(obs['value_numeric'])
                elif obs['value_datetime'] is not None:
                    actual_value = str(obs['value_datetime'])
                elif obs['value_text'] is not None:
                    actual_value = obs['value_text']

                data.append((
                    obs['obs_id'], obs['person_id'], obs['encounter_id'], obs['order_id'],
                    obs['obs_datetime'], obs['location_id'], obs['location_name'], obs['obs_group_id'],
                    obs['concept_id'], obs['concept_name'], obs['datatype_id'], obs['datatype_name'],
                    obs['value_coded'], obs['value_coded_name_id'], obs['value_drug'],
                    obs['value_datetime'], obs['value_numeric'], obs['value_text'], obs['value_complex'],
                    obs['comments'], obs['creator'], obs['creator_name'],
                    obs['date_created'], obs['date_changed'], obs['uuid'], actual_value
                ))

            # Use efficient bulk insert
            if self.incremental:
                insert_query = """
                    INSERT INTO observations (
                        obs_id, person_id, encounter_id, order_id, obs_datetime, location_id, location_name,
                        obs_group_id, concept_id, concept_name, datatype_id, datatype_name,
                        value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric,
                        value_text, value_complex, comments, creator, creator_name,
                        date_created, date_changed, uuid, actual_value
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        obs_datetime = VALUES(obs_datetime),
                        location_id = VALUES(location_id),
                        location_name = VALUES(location_name),
                        value_coded = VALUES(value_coded),
                        value_coded_name_id = VALUES(value_coded_name_id),
                        value_drug = VALUES(value_drug),
                        value_datetime = VALUES(value_datetime),
                        value_numeric = VALUES(value_numeric),
                        value_text = VALUES(value_text),
                        value_complex = VALUES(value_complex),
                        comments = VALUES(comments),
                        date_changed = VALUES(date_changed),
                        actual_value = VALUES(actual_value)
                """
            else:
                with self.get_db_connection() as conn:
                    execute_query(conn, "TRUNCATE TABLE observations")
                insert_query = """
                    INSERT INTO observations (
                        obs_id, person_id, encounter_id, order_id, obs_datetime, location_id, location_name,
                        obs_group_id, concept_id, concept_name, datatype_id, datatype_name,
                        value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric,
                        value_text, value_complex, comments, creator, creator_name,
                        date_created, date_changed, uuid, actual_value
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            # Execute bulk insert in batches to avoid memory issues
            batch_size = 1000
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        cursor.executemany(insert_query, batch)
                    conn.commit()

            # Update metadata
            self._update_metadata('observations', len(observations))

            # Mark as complete
            with self.output().open('w') as f:
                f.write(f'Observations loaded successfully: {len(observations)} records\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

    def _update_metadata(self, table_name, record_count):
        """Update ETL metadata table."""
        query = """
            INSERT INTO etl_metadata (table_name, record_count, status, updated_at)
            VALUES (%s, %s, 'completed', NOW())
            ON DUPLICATE KEY UPDATE
                record_count = VALUES(record_count),
                status = 'completed',
                updated_at = NOW()
        """
        with self.get_db_connection() as conn:
            execute_query(conn, query, (table_name, record_count))

class LoadLocationsTask(TargetDatabaseTask):
    """
    Load location data into target database.
    """
    incremental = luigi.BoolParameter(default=False)
    last_updated = luigi.DateParameter(default=None)

    def requires(self):
        deps = _task_dependencies.get('LoadLocationsTask', [])
        # Import required modules dynamically
        from tasks import schema_tasks
        from tasks.dynamic_task_factory import _dynamic_tasks

        required_tasks = []
        for dep in deps:
            if dep in _dynamic_tasks:
                required_tasks.append(_dynamic_tasks[dep]())
            elif hasattr(schema_tasks, dep):
                required_tasks.append(getattr(schema_tasks, dep)())

        return required_tasks


    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'locations_loaded.txt'))

    def run(self):
        try:
            log_task_start(self)

            # Read extracted data
            with self.input()[1].open('r') as f:
                locations = json.load(f)

            if not locations:
                self.logger.info("No location data to load")
                with self.output().open('w') as f:
                    f.write('No locations to load\n')
                return

            # Prepare data for insertion
            data = [(
                l['location_id'], l['name'], l['description'], l['address1'], l['address2'],
                l['city_village'], l['state_province'], l['postal_code'], l['country'],
                l['latitude'], l['longitude'], l['creator'], l['creator_name'],
                l['date_created'], l['date_changed'], l['uuid'],
                l['location_type_id'], l['location_type_name']
            ) for l in locations]

            # Use efficient bulk insert
            if self.incremental:
                insert_query = """
                    INSERT INTO locations (
                        location_id, name, description, address1, address2, city_village,
                        state_province, postal_code, country, latitude, longitude,
                        creator, creator_name, date_created, date_changed, uuid,
                        location_type_id, location_type_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        description = VALUES(description),
                        address1 = VALUES(address1),
                        address2 = VALUES(address2),
                        city_village = VALUES(city_village),
                        state_province = VALUES(state_province),
                        postal_code = VALUES(postal_code),
                        country = VALUES(country),
                        latitude = VALUES(latitude),
                        longitude = VALUES(longitude),
                        date_changed = VALUES(date_changed)
                """
            else:
                with self.get_db_connection() as conn:
                    execute_query(conn, "TRUNCATE TABLE locations")
                insert_query = """
                    INSERT INTO locations (
                        location_id, name, description, address1, address2, city_village,
                        state_province, postal_code, country, latitude, longitude,
                        creator, creator_name, date_created, date_changed, uuid,
                        location_type_id, location_type_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            # Execute bulk insert
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, data)
                    conn.commit()

            # Update metadata
            self._update_metadata('locations', len(locations))

            # Mark as complete
            with self.output().open('w') as f:
                f.write(f'Locations loaded successfully: {len(locations)} records\n')

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

    def _update_metadata(self, table_name, record_count):
        """Update ETL metadata table."""
        query = """
            INSERT INTO etl_metadata (table_name, record_count, status, updated_at)
            VALUES (%s, %s, 'completed', NOW())
            ON DUPLICATE KEY UPDATE
                record_count = VALUES(record_count),
                status = 'completed',
                updated_at = NOW()
        """
        with self.get_db_connection() as conn:
            execute_query(conn, query, (table_name, record_count))