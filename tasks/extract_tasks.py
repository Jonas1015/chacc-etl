import luigi
import json
import os
from tasks.base_tasks import SourceDatabaseTask
from utils import log_task_start, log_task_complete, log_task_error, execute_query
from config import DATA_DIR

class ExtractPatientsTask(SourceDatabaseTask):
    """
    Extract patient data from OpenMRS database.
    """
    last_updated = luigi.DateParameter(default=None)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'patients.json'))

    def run(self):
        try:
            log_task_start(self)

            query = """
                SELECT
                    p.patient_id,
                    p.patient_id as patient_identifier,
                    pi.identifier as openmrs_id,
                    pn.given_name,
                    pn.middle_name,
                    pn.family_name,
                    pr.birthdate,
                    pr.birthdate_estimated,
                    pr.gender,
                    pr.dead,
                    pr.death_date,
                    p.date_created,
                    p.date_changed,
                    p.uuid
                FROM patient p
                LEFT JOIN person pr ON p.patient_id = pr.person_id
                LEFT JOIN patient_identifier pi ON p.patient_id = pi.patient_id
                    AND pi.identifier_type = 3
                    AND pi.voided = 0
                LEFT JOIN person_name pn ON p.patient_id = pn.person_id
                    AND pn.voided = 0
                WHERE p.voided = 0 AND pr.voided = 0
            """

            params = []
            if self.last_updated:
                query += " AND (p.date_changed > %s OR pr.date_changed > %s)"
                params.extend([self.last_updated, self.last_updated])

            query += " ORDER BY p.patient_id"

            with self.get_db_connection() as conn:
                patients = execute_query(conn, query, params, fetch=True)

            if patients is None:
                patients = []

            with self.output().open('w') as f:
                json.dump(patients, f, indent=2, default=str)

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class ExtractEncountersTask(SourceDatabaseTask):
    """
    Extract encounter data from OpenMRS database.
    """
    last_updated = luigi.DateParameter(default=None)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'encounters.json'))

    def run(self):
        try:
            log_task_start(self)

            query = """
                SELECT
                    e.encounter_id,
                    e.patient_id,
                    e.encounter_type,
                    et.name as encounter_type_name,
                    e.form_id,
                    f.name as form_name,
                    f.uuid as form_uuid,
                    e.encounter_datetime,
                    ep.provider_id,
                    CONCAT(pn.given_name, ' ', pn.family_name) as provider_name,
                    e.location_id,
                    l.name as location_name,
                    e.date_created,
                    e.date_changed,
                    e.uuid
                FROM encounter e
                LEFT JOIN encounter_type et ON e.encounter_type = et.encounter_type_id
                LEFT JOIN form f ON e.form_id = f.form_id
                LEFT JOIN encounter_provider ep ON e.encounter_id = ep.encounter_id AND ep.voided = 0
                LEFT JOIN users u ON ep.provider_id = u.user_id
                LEFT JOIN person_name pn ON u.person_id = pn.person_id AND pn.voided = 0
                LEFT JOIN location l ON e.location_id = l.location_id
                WHERE e.voided = 0
            """

            params = []
            if self.last_updated:
                query += " AND e.date_changed > %s"
                params.append(self.last_updated)

            query += " ORDER BY e.encounter_id"

            with self.get_db_connection() as conn:
                encounters = execute_query(conn, query, params, fetch=True)

            if encounters is None:
                encounters = []

            with self.output().open('w') as f:
                json.dump(encounters, f, indent=2, default=str)

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class ExtractObservationsTask(SourceDatabaseTask):
    """
    Extract observation (obs) data from OpenMRS database.
    """
    last_updated = luigi.DateParameter(default=None)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'observations.json'))

    def run(self):
        try:
            log_task_start(self)

            query = """
                SELECT
                    o.obs_id,
                    o.person_id,
                    o.encounter_id,
                    o.order_id,
                    o.obs_datetime,
                    o.location_id,
                    l.name as location_name,
                    o.obs_group_id,
                    o.concept_id,
                    c.short_name as concept_name,
                    c.datatype_id,
                    dt.name as datatype_name,
                    o.value_coded,
                    o.value_coded_name_id,
                    o.value_drug,
                    o.value_datetime,
                    o.value_numeric,
                    o.value_text,
                    o.value_complex,
                    o.comments,
                    o.creator,
                    CONCAT(pn.given_name, ' ', pn.family_name) as creator_name,
                    o.date_created,
                    o.uuid
                FROM obs o
                LEFT JOIN concept c ON o.concept_id = c.concept_id
                LEFT JOIN concept_datatype dt ON c.datatype_id = dt.concept_datatype_id
                LEFT JOIN location l ON o.location_id = l.location_id
                LEFT JOIN users u ON o.creator = u.user_id
                LEFT JOIN person_name pn ON u.person_id = pn.person_id AND pn.voided = 0
                WHERE o.voided = 0
            """

            params = []
            if self.last_updated:
                query += " AND o.date_changed > %s"
                params.append(self.last_updated)

            query += " ORDER BY o.obs_id"

            with self.get_db_connection() as conn:
                observations = execute_query(conn, query, params, fetch=True)

            if observations is None:
                observations = []

            with self.output().open('w') as f:
                json.dump(observations, f, indent=2, default=str)

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise

class ExtractLocationsTask(SourceDatabaseTask):
    """
    Extract location/facility data from OpenMRS database.
    """
    last_updated = luigi.DateParameter(default=None)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'locations.json'))

    def run(self):
        try:
            log_task_start(self)

            query = """
                SELECT
                    l.location_id,
                    l.name,
                    l.description,
                    l.address1,
                    l.address2,
                    l.city_village,
                    l.state_province,
                    l.postal_code,
                    l.country,
                    l.latitude,
                    l.longitude,
                    l.creator,
                    CONCAT(pn.given_name, ' ', pn.family_name) as creator_name,
                    l.date_created,
                    l.date_changed,
                    l.uuid
                FROM location l
                LEFT JOIN users u ON l.creator = u.user_id
                LEFT JOIN person_name pn ON u.person_id = pn.person_id AND pn.voided = 0
                WHERE l.retired = 0
            """

            params = []
            if self.last_updated:
                query += " AND l.date_changed > %s"
                params.append(self.last_updated)

            query += " ORDER BY l.location_id"

            with self.get_db_connection() as conn:
                locations = execute_query(conn, query, params, fetch=True)

            if locations is None:
                locations = []

            with self.output().open('w') as f:
                json.dump(locations, f, indent=2, default=str)

            log_task_complete(self)
        except Exception as e:
            log_task_error(self, e)
            raise