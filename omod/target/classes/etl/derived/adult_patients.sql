SELECT
    p.patient_id,
    pn.given_name,
    pn.family_name,
    pe.gender,
    pe.birthdate,
    TIMESTAMPDIFF(YEAR, pe.birthdate, CURDATE()) AS age
FROM patient p
JOIN person pe ON p.patient_id = pe.person_id AND pe.voided = 0
JOIN person_name pn ON p.patient_id = pn.person_id AND pn.voided = 0
  WHERE TIMESTAMPDIFF(YEAR, pe.birthdate, CURDATE()) >= 18
  AND p.voided = 0;