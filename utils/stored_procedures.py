"""
Stored procedures for efficient data migration and flattening.
"""

# Stored procedure for creating flattened patient encounters table
CREATE_FLATTENED_PATIENT_ENCOUNTERS_PROC = """
CREATE PROCEDURE IF NOT EXISTS sp_create_flattened_patient_encounters()
BEGIN
    DROP TABLE IF EXISTS flattened_patient_encounters;

    CREATE TABLE flattened_patient_encounters AS
    SELECT
        p.patient_id,
        p.patient_identifier,
        p.openmrs_id,
        CONCAT_WS(' ', p.given_name, p.middle_name, p.family_name) as patient_name,
        p.birthdate,
        p.birthdate_estimated,
        p.gender,
        p.dead,
        p.death_date,
        e.encounter_id,
        e.encounter_type,
        e.encounter_type_name,
        e.form_id,
        e.form_name,
        e.form_uuid,
        e.encounter_datetime,
        e.provider_id,
        e.provider_name,
        e.location_id,
        e.location_name,
        e.date_created as encounter_created,
        e.date_changed as encounter_changed,
        e.uuid as encounter_uuid,
        p.date_created as patient_created,
        p.date_changed as patient_changed,
        p.uuid as patient_uuid
    FROM patients p
    INNER JOIN encounters e ON p.patient_id = e.patient_id
    ORDER BY p.patient_id, e.encounter_datetime;

    CREATE INDEX idx_fpe_patient_id ON flattened_patient_encounters (patient_id);
    CREATE INDEX idx_fpe_encounter_datetime ON flattened_patient_encounters (encounter_datetime);
    CREATE INDEX idx_fpe_location_id ON flattened_patient_encounters (location_id);
    CREATE INDEX idx_fpe_encounter_type ON flattened_patient_encounters (encounter_type);
END
"""

# Stored procedure for creating flattened observations table
CREATE_FLATTENED_OBSERVATIONS_PROC = """
CREATE PROCEDURE IF NOT EXISTS sp_create_flattened_observations()
BEGIN
    DROP TABLE IF EXISTS flattened_observations;

    CREATE TABLE flattened_observations AS
    SELECT
        o.obs_id,
        o.person_id as patient_id,
        p.patient_identifier,
        p.openmrs_id,
        CONCAT_WS(' ', p.given_name, p.middle_name, p.family_name) as patient_name,
        p.gender,
        p.birthdate,
        o.encounter_id,
        e.encounter_type_name,
        e.encounter_datetime,
        o.obs_datetime,
        o.location_id,
        l.name as location_name,
        o.concept_id,
        o.concept_name,
        o.datatype_name,
        o.value_coded,
        o.value_coded_name_id,
        o.value_drug,
        o.value_datetime,
        o.value_numeric,
        o.value_text,
        o.value_complex,
        o.comments,
        o.creator,
        o.creator_name,
        o.date_created as obs_created,
        o.date_changed as obs_changed,
        o.uuid as obs_uuid,
        CASE
            WHEN o.value_coded IS NOT NULL THEN CAST(o.value_coded AS CHAR)
            WHEN o.value_numeric IS NOT NULL THEN CAST(o.value_numeric AS CHAR)
            WHEN o.value_datetime IS NOT NULL THEN DATE_FORMAT(o.value_datetime, '%Y-%m-%d %H:%i:%s')
            WHEN o.value_text IS NOT NULL THEN o.value_text
            ELSE NULL
        END as actual_value
    FROM observations o
    LEFT JOIN patients p ON o.person_id = p.patient_id
    LEFT JOIN encounters e ON o.encounter_id = e.encounter_id
    LEFT JOIN locations l ON o.location_id = l.location_id
    ORDER BY o.person_id, o.obs_datetime;

    CREATE INDEX idx_fo_patient_id ON flattened_observations (patient_id);
    CREATE INDEX idx_fo_concept_id ON flattened_observations (concept_id);
    CREATE INDEX idx_fo_obs_datetime ON flattened_observations (obs_datetime);
    CREATE INDEX idx_fo_encounter_id ON flattened_observations (encounter_id);
    CREATE INDEX idx_fo_location_id ON flattened_observations (location_id);
END
"""

# Stored procedure for incremental patient updates
INCREMENTAL_PATIENT_UPDATE_PROC = """
CREATE PROCEDURE IF NOT EXISTS sp_incremental_patient_update(IN last_update_timestamp DATETIME)
BEGIN
    INSERT INTO patients (
        patient_id, patient_identifier, openmrs_id, given_name, middle_name, family_name,
        birthdate, birthdate_estimated, gender, dead, death_date, date_created, date_changed, uuid
    )
    SELECT
        p.patient_id,
        p.patient_id as patient_identifier,
        pi.identifier as openmrs_id,
        pn.given_name,
        pn.middle_name,
        pn.family_name,
        p.birthdate,
        p.birthdate_estimated,
        p.gender,
        p.dead,
        p.death_date,
        p.date_created,
        p.date_changed,
        p.uuid
    FROM source_db.patient p
    LEFT JOIN source_db.patient_identifier pi ON p.patient_id = pi.patient_id
        AND pi.identifier_type = 3 AND pi.voided = 0
    LEFT JOIN source_db.person_name pn ON p.patient_id = pn.person_id AND pn.voided = 0
    WHERE p.voided = 0 AND p.date_changed > last_update_timestamp
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
        date_changed = VALUES(date_changed);
END
"""

# Stored procedure for incremental encounter updates
INCREMENTAL_ENCOUNTER_UPDATE_PROC = """
CREATE PROCEDURE IF NOT EXISTS sp_incremental_encounter_update(IN last_update_timestamp DATETIME)
BEGIN
    INSERT INTO encounters (
        encounter_id, patient_id, encounter_type, encounter_type_name, form_id, form_name, form_uuid,
        encounter_datetime, provider_id, provider_name, location_id, location_name,
        date_created, date_changed, uuid
    )
    SELECT
        e.encounter_id,
        e.patient_id,
        e.encounter_type,
        et.name as encounter_type_name,
        e.form_id,
        f.name as form_name,
        f.uuid as form_uuid,
        e.encounter_datetime,
        e.provider_id,
        CONCAT(pn.given_name, ' ', pn.family_name) as provider_name,
        e.location_id,
        l.name as location_name,
        e.date_created,
        e.date_changed,
        e.uuid
    FROM source_db.encounter e
    LEFT JOIN source_db.encounter_type et ON e.encounter_type = et.encounter_type_id
    LEFT JOIN source_db.form f ON e.form_id = f.form_id
    LEFT JOIN source_db.users u ON e.provider_id = u.user_id
    LEFT JOIN source_db.person_name pn ON u.person_id = pn.person_id AND pn.voided = 0
    LEFT JOIN source_db.location l ON e.location_id = l.location_id
    WHERE e.voided = 0 AND e.date_changed > last_update_timestamp
    ON DUPLICATE KEY UPDATE
        encounter_datetime = VALUES(encounter_datetime),
        provider_id = VALUES(provider_id),
        provider_name = VALUES(provider_name),
        location_id = VALUES(location_id),
        location_name = VALUES(location_name),
        date_changed = VALUES(date_changed);
END
"""

# Stored procedure for incremental observation updates
INCREMENTAL_OBSERVATION_UPDATE_PROC = """
CREATE PROCEDURE IF NOT EXISTS sp_incremental_observation_update(IN last_update_timestamp DATETIME)
BEGIN
    INSERT INTO observations (
        obs_id, person_id, encounter_id, order_id, obs_datetime, location_id, location_name,
        obs_group_id, concept_id, concept_name, datatype_id, datatype_name,
        value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_text, value_complex,
        comments, creator, creator_name, date_created, date_changed, uuid
    )
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
        o.date_changed,
        o.uuid
    FROM source_db.obs o
    LEFT JOIN source_db.concept c ON o.concept_id = c.concept_id
    LEFT JOIN source_db.concept_datatype dt ON c.datatype_id = dt.concept_datatype_id
    LEFT JOIN source_db.location l ON o.location_id = l.location_id
    LEFT JOIN source_db.users u ON o.creator = u.user_id
    LEFT JOIN source_db.person_name pn ON u.person_id = pn.person_id AND pn.voided = 0
    WHERE o.voided = 0 AND o.date_changed > last_update_timestamp
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
        date_changed = VALUES(date_changed);
END
"""

# List of all stored procedures
ALL_PROCEDURES = [
    CREATE_FLATTENED_PATIENT_ENCOUNTERS_PROC,
    CREATE_FLATTENED_OBSERVATIONS_PROC,
    INCREMENTAL_PATIENT_UPDATE_PROC,
    INCREMENTAL_ENCOUNTER_UPDATE_PROC,
    INCREMENTAL_OBSERVATION_UPDATE_PROC,
]

# Procedure names for reference
PROCEDURE_NAMES = [
    'sp_create_flattened_patient_encounters',
    'sp_create_flattened_observations',
    'sp_incremental_patient_update',
    'sp_incremental_encounter_update',
    'sp_incremental_observation_update',
]