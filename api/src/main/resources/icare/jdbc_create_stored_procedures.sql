CREATE database IF NOT EXISTS icare_etl_db;
~-~-

USE icare_etl_db;
~-~-


        
    
        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_calculate_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_calculate_agegroup;


~-~-
CREATE FUNCTION fn_icare_calculate_agegroup(age INT) RETURNS VARCHAR(15)
    DETERMINISTIC
BEGIN
    DECLARE agegroup VARCHAR(15);
    CASE
        WHEN age < 1 THEN SET agegroup = '<1';
        WHEN age between 1 and 4 THEN SET agegroup = '1-4';
        WHEN age between 5 and 9 THEN SET agegroup = '5-9';
        WHEN age between 10 and 14 THEN SET agegroup = '10-14';
        WHEN age between 15 and 19 THEN SET agegroup = '15-19';
        WHEN age between 20 and 24 THEN SET agegroup = '20-24';
        WHEN age between 25 and 29 THEN SET agegroup = '25-29';
        WHEN age between 30 and 34 THEN SET agegroup = '30-34';
        WHEN age between 35 and 39 THEN SET agegroup = '35-39';
        WHEN age between 40 and 44 THEN SET agegroup = '40-44';
        WHEN age between 45 and 49 THEN SET agegroup = '45-49';
        WHEN age between 50 and 54 THEN SET agegroup = '50-54';
        WHEN age between 55 and 59 THEN SET agegroup = '55-59';
        WHEN age between 60 and 64 THEN SET agegroup = '60-64';
        ELSE SET agegroup = '65+';
        END CASE;

    RETURN agegroup;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_get_obs_value_column  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_get_obs_value_column;


~-~-
CREATE FUNCTION fn_icare_get_obs_value_column(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE obsValueColumn VARCHAR(20);

        IF conceptDatatype = 'Text' THEN
            SET obsValueColumn = 'obs_value_text';

        ELSEIF conceptDatatype = 'Coded'
           OR conceptDatatype = 'N/A' THEN
            SET obsValueColumn = 'obs_value_text';

        ELSEIF conceptDatatype = 'Boolean' THEN
            SET obsValueColumn = 'obs_value_boolean';

        ELSEIF  conceptDatatype = 'Date'
                OR conceptDatatype = 'Datetime' THEN
            SET obsValueColumn = 'obs_value_datetime';

        ELSEIF conceptDatatype = 'Numeric' THEN
            SET obsValueColumn = 'obs_value_numeric';

        ELSE
            SET obsValueColumn = 'obs_value_text';

        END IF;

    RETURN (obsValueColumn);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_age_calculator  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_age_calculator;


~-~-
CREATE FUNCTION fn_icare_age_calculator(birthdate DATE, deathDate DATE) RETURNS INTEGER
    DETERMINISTIC
BEGIN
    DECLARE today DATE;
    DECLARE age INT;

    -- Check if birthdate is not null and not an empty string
    IF birthdate IS NULL OR TRIM(birthdate) = '' THEN
        RETURN NULL;
    ELSE
        SET today = IFNULL(CURDATE(), '0000-00-00');
        -- Check if birthdate is a valid date using STR_TO_DATE and if it's not in the future
        IF STR_TO_DATE(birthdate, '%Y-%m-%d') IS NULL OR STR_TO_DATE(birthdate, '%Y-%m-%d') > today THEN
            RETURN NULL;
        END IF;

        -- If deathDate is provided and in the past, set today to deathDate
        IF deathDate IS NOT NULL AND today > deathDate THEN
            SET today = deathDate;
        END IF;

        SET age = YEAR(today) - YEAR(birthdate);

        -- Adjust age based on month and day
        IF MONTH(today) < MONTH(birthdate) OR (MONTH(today) = MONTH(birthdate) AND DAY(today) < DAY(birthdate)) THEN
            SET age = age - 1;
        END IF;

        RETURN age;
    END IF;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_get_datatype_for_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_get_datatype_for_concept;


~-~-
CREATE FUNCTION fn_icare_get_datatype_for_concept(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE mysqlDatatype VARCHAR(20);


    IF conceptDatatype = 'Text' THEN
        SET mysqlDatatype = 'TEXT';

    ELSEIF conceptDatatype = 'Coded'
        OR conceptDatatype = 'N/A' THEN
        SET mysqlDatatype = 'VARCHAR(250)';

    ELSEIF conceptDatatype = 'Boolean' THEN
        SET mysqlDatatype = 'BOOLEAN';

    ELSEIF conceptDatatype = 'Date' THEN
        SET mysqlDatatype = 'DATE';

    ELSEIF conceptDatatype = 'Datetime' THEN
        SET mysqlDatatype = 'DATETIME';

    ELSEIF conceptDatatype = 'Numeric' THEN
        SET mysqlDatatype = 'DOUBLE';

    ELSE
        SET mysqlDatatype = 'TEXT';

    END IF;

    RETURN mysqlDatatype;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_generate_json_from_icare_flat_table_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_generate_json_from_icare_flat_table_config;


~-~-
CREATE FUNCTION fn_icare_generate_json_from_icare_flat_table_config(
    is_incremental TINYINT(1)
) RETURNS JSON
    DETERMINISTIC
BEGIN
    DECLARE report_array JSON;
    SET session group_concat_max_len = 200000;

    SELECT CONCAT('{"flat_report_metadata":[', GROUP_CONCAT(
            CONCAT(
                    '{',
                    '"report_name":', JSON_EXTRACT(table_json_data, '$.report_name'),
                    ',"flat_table_name":', JSON_EXTRACT(table_json_data, '$.flat_table_name'),
                    ',"encounter_type_uuid":', JSON_EXTRACT(table_json_data, '$.encounter_type_uuid'),
                    ',"table_columns": ', JSON_EXTRACT(table_json_data, '$.table_columns'),
                    '}'
            ) SEPARATOR ','), ']}')
    INTO report_array
    FROM icare_flat_table_config
    WHERE (IF(is_incremental = 1, incremental_record = 1, 1));

    RETURN report_array;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_array_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_array_length;

~-~-
CREATE FUNCTION fn_icare_array_length(array_string TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
  DECLARE length INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  -- If the array_string is not empty, initialize length to 1
    IF TRIM(array_string) != '' AND TRIM(array_string) != '[]' THEN
        SET length = 1;
    END IF;

  -- Count the number of commas in the array string
    WHILE i <= CHAR_LENGTH(array_string) DO
        IF SUBSTRING(array_string, i, 1) = ',' THEN
          SET length = length + 1;
        END IF;
        SET i = i + 1;
    END WHILE;

RETURN length;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_get_array_item_by_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_get_array_item_by_index;

~-~-
CREATE FUNCTION fn_icare_get_array_item_by_index(array_string TEXT, item_index INT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE elem_start INT DEFAULT 1;
  DECLARE elem_end INT DEFAULT 0;
  DECLARE current_index INT DEFAULT 0;
  DECLARE result TEXT DEFAULT '';

    -- If the item_index is less than 1 or the array_string is empty, return an empty string
    IF item_index < 1 OR array_string = '[]' OR TRIM(array_string) = '' THEN
        RETURN '';
    END IF;

    -- Loop until we find the start quote of the desired index
    WHILE current_index < item_index DO
        -- Find the start quote of the next element
        SET elem_start = LOCATE('"', array_string, elem_end + 1);
        -- If we can't find a new element, return an empty string
        IF elem_start = 0 THEN
          RETURN '';
        END IF;

        -- Find the end quote of this element
        SET elem_end = LOCATE('"', array_string, elem_start + 1);
        -- If we can't find the end quote, return an empty string
        IF elem_end = 0 THEN
          RETURN '';
        END IF;

        -- Increment the current_index
        SET current_index = current_index + 1;
    END WHILE;

    -- When the loop exits, current_index should equal item_index, and elem_start/end should be the positions of the quotes
    -- Extract the element
    SET result = SUBSTRING(array_string, elem_start + 1, elem_end - elem_start - 1);

    RETURN result;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_array_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_array_length;

~-~-
CREATE FUNCTION fn_icare_json_array_length(json_array TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE array_length INT DEFAULT 0;
    DECLARE current_pos INT DEFAULT 1;
    DECLARE char_val CHAR(1);

    IF json_array IS NULL THEN
        RETURN 0;
    END IF;

  -- Iterate over the string to count the number of objects based on commas and curly braces
    WHILE current_pos <= CHAR_LENGTH(json_array) DO
        SET char_val = SUBSTRING(json_array, current_pos, 1);

    -- Check for the start of an object
        IF char_val = '{' THEN
            SET array_length = array_length + 1;

      -- Move current_pos to the end of this object
            SET current_pos = LOCATE('}', json_array, current_pos) + 1;
        ELSE
            SET current_pos = current_pos + 1;
        END IF;
    END WHILE;

RETURN array_length;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_extract  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_extract;

~-~-
CREATE FUNCTION fn_icare_json_extract(json TEXT, key_name VARCHAR(255)) RETURNS VARCHAR(255)
    DETERMINISTIC
BEGIN
  DECLARE start_index INT;
  DECLARE end_index INT;
  DECLARE key_length INT;
  DECLARE key_index INT;

  SET key_name = CONCAT( key_name, '":');
  SET key_length = CHAR_LENGTH(key_name);
  SET key_index = LOCATE(key_name, json);

    IF key_index = 0 THEN
        RETURN NULL;
    END IF;

    SET start_index = key_index + key_length;

    CASE
        WHEN SUBSTRING(json, start_index, 1) = '"' THEN
            SET start_index = start_index + 1;
            SET end_index = LOCATE('"', json, start_index);
        ELSE
            SET end_index = LOCATE(',', json, start_index);
            IF end_index = 0 THEN
                SET end_index = LOCATE('}', json, start_index);
            END IF;
    END CASE;

RETURN SUBSTRING(json, start_index, end_index - start_index);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_extract_array  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_extract_array;

~-~-
CREATE FUNCTION fn_icare_json_extract_array(json TEXT, key_name VARCHAR(255)) RETURNS TEXT
    DETERMINISTIC
BEGIN
DECLARE start_index INT;
DECLARE end_index INT;
DECLARE array_text TEXT;

    SET key_name = CONCAT('"', key_name, '":');
    SET start_index = LOCATE(key_name, json);

    IF start_index = 0 THEN
        RETURN NULL;
    END IF;

    SET start_index = start_index + CHAR_LENGTH(key_name);

    IF SUBSTRING(json, start_index, 1) != '[' THEN
        RETURN NULL;
    END IF;

    SET start_index = start_index + 1; -- Start after the '['
    SET end_index = start_index;

    -- Loop to find the matching closing bracket for the array
    SET @bracket_counter = 1;
    WHILE @bracket_counter > 0 AND end_index <= CHAR_LENGTH(json) DO
        SET end_index = end_index + 1;
        IF SUBSTRING(json, end_index, 1) = '[' THEN
          SET @bracket_counter = @bracket_counter + 1;
        ELSEIF SUBSTRING(json, end_index, 1) = ']' THEN
          SET @bracket_counter = @bracket_counter - 1;
        END IF;
    END WHILE;

    IF @bracket_counter != 0 THEN
        RETURN NULL; -- The brackets are not balanced, return NULL
    END IF;

SET array_text = SUBSTRING(json, start_index, end_index - start_index);

RETURN array_text;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_extract_object  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_extract_object;

~-~-
CREATE FUNCTION fn_icare_json_extract_object(json_string TEXT, key_name VARCHAR(255)) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE start_index INT;
  DECLARE end_index INT;
  DECLARE nested_level INT DEFAULT 0;
  DECLARE substring_length INT;
  DECLARE key_str VARCHAR(255);
  DECLARE result TEXT DEFAULT '';

  SET key_str := CONCAT('"', key_name, '": {');

  -- Find the start position of the key
  SET start_index := LOCATE(key_str, json_string);
    IF start_index = 0 THEN
        RETURN NULL;
    END IF;

    -- Adjust start_index to the start of the value
    SET start_index := start_index + CHAR_LENGTH(key_str);

    -- Initialize the end_index to start_index
    SET end_index := start_index;

    -- Find the end of the object
    WHILE nested_level >= 0 AND end_index <= CHAR_LENGTH(json_string) DO
        SET end_index := end_index + 1;
        SET substring_length := end_index - start_index;

        -- Check for nested objects
        IF SUBSTRING(json_string, end_index, 1) = '{' THEN
          SET nested_level := nested_level + 1;
        ELSEIF SUBSTRING(json_string, end_index, 1) = '}' THEN
          SET nested_level := nested_level - 1;
        END IF;
    END WHILE;

    -- Get the JSON object
    IF nested_level < 0 THEN
    -- We found a matching pair of curly braces
        SET result := SUBSTRING(json_string, start_index, substring_length);
    END IF;

RETURN result;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_keys_array  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_icare_json_keys_array;

~-~-
CREATE FUNCTION fn_icare_json_keys_array(json_object TEXT) RETURNS TEXT
    DETERMINISTIC
BEGIN
    DECLARE finished INT DEFAULT 0;
    DECLARE start_index INT DEFAULT 1;
    DECLARE end_index INT DEFAULT 1;
    DECLARE key_name TEXT DEFAULT '';
    DECLARE my_keys TEXT DEFAULT '';
    DECLARE json_length INT;
    DECLARE key_end_index INT;

    SET json_length = CHAR_LENGTH(json_object);

    -- Initialize the my_keys string as an empty 'array'
    SET my_keys = '';

    -- This loop goes through the JSON object and extracts the my_keys
    WHILE NOT finished DO
            -- Find the start of the key
            SET start_index = LOCATE('"', json_object, end_index);
            IF start_index = 0 OR start_index >= json_length THEN
                SET finished = 1;
            ELSE
                -- Find the end of the key
                SET end_index = LOCATE('"', json_object, start_index + 1);
                SET key_name = SUBSTRING(json_object, start_index + 1, end_index - start_index - 1);

                -- Append the key to the 'array' of my_keys
                IF my_keys = ''
                    THEN
                    SET my_keys = CONCAT('["', key_name, '"');
                ELSE
                    SET my_keys = CONCAT(my_keys, ',"', key_name, '"');
                END IF;

                -- Move past the current key-value pair
                SET key_end_index = LOCATE(',', json_object, end_index);
                IF key_end_index = 0 THEN
                    SET key_end_index = LOCATE('}', json_object, end_index);
                END IF;
                IF key_end_index = 0 THEN
                    -- Closing brace not found - malformed JSON
                    SET finished = 1;
                ELSE
                    -- Prepare for the next iteration
                    SET end_index = key_end_index + 1;
                END IF;
            END IF;
    END WHILE;

    -- Close the 'array' of my_keys
    IF my_keys != '' THEN
        SET my_keys = CONCAT(my_keys, ']');
    END IF;

    RETURN my_keys;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_length;

~-~-
CREATE FUNCTION fn_icare_json_length(json_array TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE element_count INT DEFAULT 0;
    DECLARE current_position INT DEFAULT 1;

    WHILE current_position <= CHAR_LENGTH(json_array) DO
        SET element_count = element_count + 1;
        SET current_position = LOCATE(',', json_array, current_position) + 1;

        IF current_position = 0 THEN
            RETURN element_count;
        END IF;
    END WHILE;

RETURN element_count;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_object_at_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_object_at_index;

~-~-
CREATE FUNCTION fn_icare_json_object_at_index(json_array TEXT, index_pos INT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE obj_start INT DEFAULT 1;
  DECLARE obj_end INT DEFAULT 1;
  DECLARE current_index INT DEFAULT 0;
  DECLARE obj_text TEXT;

    -- Handle negative index_pos or json_array being NULL
    IF index_pos < 1 OR json_array IS NULL THEN
        RETURN NULL;
    END IF;

    -- Find the start of the requested object
    WHILE obj_start < CHAR_LENGTH(json_array) AND current_index < index_pos DO
        SET obj_start = LOCATE('{', json_array, obj_end);

        -- If we can't find a new object, return NULL
        IF obj_start = 0 THEN
          RETURN NULL;
        END IF;

        SET current_index = current_index + 1;
        -- If this isn't the object we want, find the end and continue
        IF current_index < index_pos THEN
          SET obj_end = LOCATE('}', json_array, obj_start) + 1;
        END IF;
    END WHILE;

    -- Now obj_start points to the start of the desired object
    -- Find the end of it
    SET obj_end = LOCATE('}', json_array, obj_start);
    IF obj_end = 0 THEN
        -- The object is not well-formed
        RETURN NULL;
    END IF;

    -- Extract the object
    SET obj_text = SUBSTRING(json_array, obj_start, obj_end - obj_start + 1);

RETURN obj_text;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_json_value_by_key  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_json_value_by_key;

~-~-
CREATE FUNCTION fn_icare_json_value_by_key(json TEXT, key_name VARCHAR(255)) RETURNS VARCHAR(255)
    DETERMINISTIC
BEGIN
    DECLARE start_index INT;
    DECLARE end_index INT;
    DECLARE key_length INT;
    DECLARE key_index INT;
    DECLARE value_length INT;
    DECLARE extracted_value VARCHAR(255);

    -- Add the key structure to search for in the JSON string
    SET key_name = CONCAT('"', key_name, '":');
    SET key_length = CHAR_LENGTH(key_name);

    -- Locate the key within the JSON string
    SET key_index = LOCATE(key_name, json);

    -- If the key is not found, return NULL
    IF key_index = 0 THEN
        RETURN NULL;
    END IF;

    -- Set the starting index of the value
    SET start_index = key_index + key_length;

    -- Check if the value is a string (starts with a quote)
    IF SUBSTRING(json, start_index, 1) = '"' THEN
        -- Set the start index to the first character of the value (skipping the quote)
        SET start_index = start_index + 1;

        -- Find the end of the string value (the next quote)
        SET end_index = LOCATE('"', json, start_index);
        IF end_index = 0 THEN
            -- If there's no end quote, the JSON is malformed
            RETURN NULL;
        END IF;
    ELSE
        -- The value is not a string (e.g., a number, boolean, or null)
        -- Find the end of the value (either a comma or closing brace)
        SET end_index = LOCATE(',', json, start_index);
        IF end_index = 0 THEN
            SET end_index = LOCATE('}', json, start_index);
        END IF;
    END IF;

    -- Calculate the length of the extracted value
    SET value_length = end_index - start_index;

    -- Extract the value
    SET extracted_value = SUBSTRING(json, start_index, value_length);

    -- Return the extracted value without leading or trailing quotes
RETURN TRIM(BOTH '"' FROM extracted_value);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_remove_all_whitespace  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_remove_all_whitespace;

~-~-
CREATE FUNCTION fn_icare_remove_all_whitespace(input_string TEXT) RETURNS TEXT
    DETERMINISTIC

BEGIN
  DECLARE cleaned_string TEXT;
  SET cleaned_string = input_string;

  -- Replace common whitespace characters
  SET cleaned_string = REPLACE(cleaned_string, CHAR(9), '');   -- Horizontal tab
  SET cleaned_string = REPLACE(cleaned_string, CHAR(10), '');  -- Line feed
  SET cleaned_string = REPLACE(cleaned_string, CHAR(13), '');  -- Carriage return
  SET cleaned_string = REPLACE(cleaned_string, CHAR(32), '');  -- Space
  -- SET cleaned_string = REPLACE(cleaned_string, CHAR(160), ''); -- Non-breaking space

RETURN TRIM(cleaned_string);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_remove_quotes  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_remove_quotes;

~-~-
CREATE FUNCTION fn_icare_remove_quotes(original TEXT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE without_quotes TEXT;

  -- Replace both single and double quotes with nothing
  SET without_quotes = REPLACE(REPLACE(original, '"', ''), '''', '');

RETURN fn_icare_remove_all_whitespace(without_quotes);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_remove_special_characters  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_remove_special_characters;


~-~-
CREATE FUNCTION fn_icare_remove_special_characters(input_text VARCHAR(255))
    RETURNS VARCHAR(255)
    DETERMINISTIC
    NO SQL
    COMMENT 'Removes special characters from input text'
BEGIN
    DECLARE modified_string VARCHAR(255);
    DECLARE special_chars VARCHAR(255);
    DECLARE char_index INT DEFAULT 1;
    DECLARE current_char CHAR(1);

    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;

    SET modified_string = input_text;

    -- Define special characters to remove
    SET special_chars = '!@#$%^&*?/,()"-=+£:;><ã\\|[]{}\'~`.'; -- TODO: Added '.' xter as well but Remove after adding backtick support

    -- Remove each special character
    WHILE char_index <= CHAR_LENGTH(special_chars) DO
            SET current_char = SUBSTRING(special_chars, char_index, 1);
            SET modified_string = REPLACE(modified_string, current_char, '');
            SET char_index = char_index + 1;
        END WHILE;

    -- Trim any leading or trailing spaces
    RETURN TRIM(modified_string);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_icare_collapse_spaces  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_icare_collapse_spaces;


~-~-
CREATE FUNCTION fn_icare_collapse_spaces(input_text TEXT)
    RETURNS TEXT
    DETERMINISTIC
BEGIN
    DECLARE result TEXT;
    SET result = input_text;

    -- First replace tabs and other whitespace characters with spaces
    SET result = REPLACE(result, '\t', ' '); -- Replace tabs with a single space

    -- Loop to collapse multiple spaces into one
    WHILE INSTR(result, '  ') > 0
        DO
            SET result = REPLACE(result, '  ', ' '); -- Replace two spaces with one space
        END WHILE;

    RETURN result;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_standardize_collation  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_standardize_collation;


~-~-
CREATE FUNCTION fn_standardize_collation(input_string VARCHAR(255))
    RETURNS VARCHAR(255) CHARSET utf8mb4 COLLATE utf8mb4_general_ci
    DETERMINISTIC
BEGIN

    RETURN input_string COLLATE utf8mb4_general_ci;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_functions_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_functions_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_stored_functions_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN
    DELETE FROM `mysql`.`proc` WHERE `type` = 'FUNCTION' AND `db` = database_name; -- works in mysql before v.8

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_stored_procedures_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_objects_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_objects_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_objects_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    CALL sp_xf_system_drop_all_stored_functions_in_schema(database_name);
    CALL sp_xf_system_drop_all_stored_procedures_in_schema(database_name);
    CALL sp_icare_system_drop_all_tables(database_name);
    # CALL sp_xf_system_drop_all_views_in_schema (database_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_system_drop_all_tables  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_system_drop_all_tables;


-- CREATE PROCEDURE sp_icare_system_drop_all_tables(IN database_name CHAR(255) CHARACTER SET UTF8MB4)
~-~-
CREATE PROCEDURE sp_icare_system_drop_all_tables()
BEGIN

    DECLARE tables_count INT;

    SET @database_name = (SELECT DATABASE());

    SELECT COUNT(1)
    INTO tables_count
    FROM information_schema.tables
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA = @database_name;

    IF tables_count > 0 THEN

        SET session group_concat_max_len = 20000;

        SET @tbls = (SELECT GROUP_CONCAT(@database_name, '.', TABLE_NAME SEPARATOR ', ')
                     FROM information_schema.tables
                     WHERE TABLE_TYPE = 'BASE TABLE'
                       AND TABLE_SCHEMA = @database_name
                       AND TABLE_NAME REGEXP '^(icare_|dim_|fact_|flat_)');

        IF (@tbls IS NOT NULL) THEN

            SET @drop_tables = CONCAT('DROP TABLE IF EXISTS ', @tbls);

            SET foreign_key_checks = 0; -- Remove check, so we don't have to drop tables in the correct order, or care if they exist or not.
            PREPARE drop_tbls FROM @drop_tables;
            EXECUTE drop_tbls;
            DEALLOCATE PREPARE drop_tbls;
            SET foreign_key_checks = 1;

        END IF;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_scheduler_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_scheduler_wrapper;


~-~-
CREATE PROCEDURE sp_icare_etl_scheduler_wrapper()

BEGIN

    DECLARE etl_ever_scheduled TINYINT(1);
    DECLARE incremental_mode TINYINT(1);
    DECLARE incremental_mode_cascaded TINYINT(1);

    SELECT COUNT(1)
    INTO etl_ever_scheduled
    FROM _icare_etl_schedule;

    SELECT incremental_mode_switch
    INTO incremental_mode
    FROM _icare_etl_user_settings;

    IF etl_ever_scheduled <= 1 OR incremental_mode = 0 THEN
        SET incremental_mode_cascaded = 0;
        CALL sp_icare_data_processing_drop_and_flatten();
    ELSE
        SET incremental_mode_cascaded = 1;
        CALL sp_icare_data_processing_increment_and_flatten();
    END IF;

    CALL sp_icare_data_processing_etl(incremental_mode_cascaded);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_schedule_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_schedule_table_create;


~-~-
CREATE PROCEDURE sp_icare_etl_schedule_table_create()
BEGIN

    CREATE TABLE IF NOT EXISTS _icare_etl_schedule
    (
        id                         INT      NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
        start_time                 DATETIME NOT NULL DEFAULT NOW(),
        end_time                   DATETIME,
        next_schedule              DATETIME,
        execution_duration_seconds BIGINT,
        missed_schedule_by_seconds BIGINT,
        completion_status          ENUM ('SUCCESS', 'ERROR'),
        transaction_status         ENUM ('RUNNING', 'COMPLETED'),
        success_or_error_message   MEDIUMTEXT,

        INDEX icare_idx_start_time (start_time),
        INDEX icare_idx_end_time (end_time),
        INDEX icare_idx_transaction_status (transaction_status),
        INDEX icare_idx_completion_status (completion_status)
    )
        CHARSET = UTF8MB4;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_schedule  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_schedule;


~-~-
CREATE PROCEDURE sp_icare_etl_schedule()

BEGIN

    DECLARE etl_execution_delay_seconds TINYINT(2) DEFAULT 0; -- 0 Seconds
    DECLARE interval_seconds INT;
    DECLARE start_time_seconds BIGINT;
    DECLARE end_time_seconds BIGINT;
    DECLARE time_now DATETIME;
    DECLARE txn_end_time DATETIME;
    DECLARE next_schedule_time DATETIME;
    DECLARE next_schedule_seconds BIGINT;
    DECLARE missed_schedule_seconds INT DEFAULT 0;
    DECLARE time_taken BIGINT;
    DECLARE etl_is_ready_to_run BOOLEAN DEFAULT FALSE;

    -- cleanup stuck schedule
    CALL sp_icare_etl_un_stuck_scheduler();
    -- check if _icare_etl_schedule is empty(new) or last transaction_status
    -- is 'COMPLETED' AND it was a 'SUCCESS' AND its 'end_time' was set.
    SET etl_is_ready_to_run = (SELECT COALESCE(
                                              (SELECT IF(end_time IS NOT NULL
                                                             AND transaction_status = 'COMPLETED'
                                                             AND completion_status = 'SUCCESS',
                                                         TRUE, FALSE)
                                               FROM _icare_etl_schedule
                                               ORDER BY id DESC
                                               LIMIT 1), TRUE));

    IF etl_is_ready_to_run THEN

        SET time_now = NOW();
        SET start_time_seconds = UNIX_TIMESTAMP(time_now);

        INSERT INTO _icare_etl_schedule(start_time, transaction_status)
        VALUES (time_now, 'RUNNING');

        SET @last_inserted_id = LAST_INSERT_ID();

        UPDATE _icare_etl_user_settings
        SET last_etl_schedule_insert_id = @last_inserted_id
        WHERE TRUE
        ORDER BY id DESC
        LIMIT 1;

        -- Call ETL
        CALL sp_icare_etl_scheduler_wrapper();

        SET txn_end_time = NOW();
        SET end_time_seconds = UNIX_TIMESTAMP(txn_end_time);

        SET time_taken = (end_time_seconds - start_time_seconds);


        SET interval_seconds = (SELECT etl_interval_seconds
                                FROM _icare_etl_user_settings
                                ORDER BY id DESC
                                LIMIT 1);

        SET next_schedule_seconds = start_time_seconds + interval_seconds + etl_execution_delay_seconds;
        SET next_schedule_time = FROM_UNIXTIME(next_schedule_seconds);

        -- Run ETL immediately if schedule was missed (give allowance of 1 second)
        IF end_time_seconds > next_schedule_seconds THEN
            SET missed_schedule_seconds = end_time_seconds - next_schedule_seconds;
            SET next_schedule_time = FROM_UNIXTIME(end_time_seconds + 1);
        END IF;

        UPDATE _icare_etl_schedule
        SET end_time                   = txn_end_time,
            next_schedule              = next_schedule_time,
            execution_duration_seconds = time_taken,
            missed_schedule_by_seconds = missed_schedule_seconds,
            completion_status          = 'SUCCESS',
            transaction_status         = 'COMPLETED'
        WHERE id = @last_inserted_id;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_schedule_trim_log_event  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_schedule_trim_log_event;


~-~-
CREATE PROCEDURE sp_icare_etl_schedule_trim_log_event()

BEGIN

    DELETE FROM _icare_etl_schedule
    WHERE id NOT IN (
        SELECT id FROM (
                           SELECT id
                           FROM _icare_etl_schedule
                           ORDER BY id DESC
                           LIMIT 20
                       ) AS recent_records
    );

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_un_stuck_scheduler  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_un_stuck_scheduler;


~-~-
CREATE PROCEDURE sp_icare_etl_un_stuck_scheduler()
BEGIN

    DECLARE running_schedule_record BOOLEAN DEFAULT FALSE;
    DECLARE no_running_icare_sp BOOLEAN DEFAULT FALSE;
    DECLARE last_schedule_record_id INT;

    SET running_schedule_record = (SELECT COALESCE(
                                                  (SELECT IF(transaction_status = 'RUNNING'
                                                                 AND completion_status is null,
                                                             TRUE, FALSE)
                                                   FROM _icare_etl_schedule
                                                   ORDER BY id DESC
                                                   LIMIT 1), TRUE));
    SET no_running_icare_sp = NOT EXISTS (SELECT 1
                                          FROM performance_schema.events_statements_current
                                          WHERE SQL_TEXT LIKE 'CALL sp_icare_etl_scheduler_wrapper(%'
                                             OR SQL_TEXT = 'CALL sp_icare_etl_scheduler_wrapper()');
    IF running_schedule_record AND no_running_icare_sp THEN
        SET last_schedule_record_id = (SELECT MAX(id) FROM _icare_etl_schedule limit 1);
        UPDATE _icare_etl_schedule
        SET end_time                   = NOW(),
            completion_status          = 'SUCCESS',
            transaction_status         = 'COMPLETED',
            success_or_error_message   = 'Stuck schedule updated'
            WHERE id = last_schedule_record_id;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_setup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_setup;


~-~-
CREATE PROCEDURE sp_icare_etl_setup(
    IN openmrs_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN etl_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    -- Setup ETL Error log Table
    CALL sp_icare_etl_error_log();

    -- Setup ETL configurations
    CALL sp_icare_etl_user_settings(openmrs_database,
                                    etl_database,
                                    concepts_locale,
                                    table_partition_number,
                                    incremental_mode_switch,
                                    automatic_flattening_mode_switch,
                                    etl_interval_seconds);

    -- create ETL schedule log table
    CALL sp_icare_etl_schedule_table_create();

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_table_create;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_table_create(
    IN flat_encounter_table_name VARCHAR(60) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', flat_encounter_table_name, '`');

    SELECT GROUP_CONCAT(CONCAT('`', column_label, '` ', fn_icare_get_datatype_for_concept(concept_datatype)) SEPARATOR ', ')
    INTO @column_labels
    FROM icare_concept_metadata
    WHERE flat_table_name = flat_encounter_table_name
      AND concept_datatype IS NOT NULL;

    IF @column_labels IS NOT NULL THEN
        SET @create_table = CONCAT(
            'CREATE TABLE `', flat_encounter_table_name, '` (`encounter_id` INT PRIMARY KEY, `visit_id` INT NULL, `client_id` INT NOT NULL, `encounter_datetime` DATETIME NOT NULL, `location_id` INT NULL, ', @column_labels, ', INDEX `icare_idx_encounter_id` (`encounter_id`), INDEX `icare_idx_visit_id` (`visit_id`), INDEX `icare_idx_client_id` (`client_id`), INDEX `icare_idx_encounter_datetime` (`encounter_datetime`), INDEX `icare_idx_location_id` (`location_id`));');
    END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE deletetb FROM @drop_table;
        PREPARE createtb FROM @create_table;

        EXECUTE deletetb;
        EXECUTE createtb;

        DEALLOCATE PREPARE deletetb;
        DEALLOCATE PREPARE createtb;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_table_create_all;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_table_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM icare_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_icare_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_table_insert;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_table_insert(
    IN p_flat_table_name VARCHAR(60) CHARACTER SET UTF8MB4,
    IN p_encounter_id INT -- Optional parameter for incremental insert
)
BEGIN

    DROP TEMPORARY TABLE IF EXISTS temp_concept_metadata;

    SET session group_concat_max_len = 20000;

    -- Handle incremental updates
    IF p_encounter_id IS NOT NULL THEN

        SET @delete_stmt = CONCAT('DELETE FROM `', p_flat_table_name, '` WHERE `encounter_id` = ?');
        PREPARE stmt FROM @delete_stmt;
        SET @encounter_id = p_encounter_id; -- Bind the variable
        EXECUTE stmt USING @encounter_id; -- Use the bound variable
        DEALLOCATE PREPARE stmt;
    END IF;

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_concept_metadata
    (
        `id`                  INT          NOT NULL,
        `flat_table_name`     VARCHAR(60)  NOT NULL,
        `encounter_type_uuid` CHAR(38)     NOT NULL,
        `column_label`        VARCHAR(255) NOT NULL,
        `concept_uuid`        CHAR(38)     NOT NULL,
        `obs_value_column`    VARCHAR(50),
        `concept_datatype`    VARCHAR(50),
        `concept_answer_obs`  INT,

        INDEX idx_id (`id`),
        INDEX idx_column_label (`column_label`),
        INDEX idx_concept_uuid (`concept_uuid`),
        INDEX idx_concept_answer_obs (`concept_answer_obs`),
        INDEX idx_flat_table_name (`flat_table_name`),
        INDEX idx_encounter_type_uuid (`encounter_type_uuid`)
    ) CHARSET = UTF8MB4;

    -- Populate metadata
    INSERT INTO temp_concept_metadata
    SELECT DISTINCT `id`,
                    `flat_table_name`,
                    `encounter_type_uuid`,
                    `column_label`,
                    `concept_uuid`,
                    fn_icare_get_obs_value_column(`concept_datatype`),
                    `concept_datatype`,
                    `concept_answer_obs`
    FROM `icare_concept_metadata`
    WHERE `flat_table_name` = p_flat_table_name
      AND `concept_id` IS NOT NULL
      AND `concept_datatype` IS NOT NULL;

    -- Generate dynamic columns
    SELECT GROUP_CONCAT(
                   DISTINCT CONCAT(
                    'MAX(CASE WHEN `column_label` = ''',
                    `column_label`,
                    ''' THEN ',
                    `obs_value_column`,
                    ' END) `',
                    `column_label`,
                    '`'
                            ) ORDER BY `id` ASC
           )
    INTO @column_labels
    FROM temp_concept_metadata;

    SELECT DISTINCT `encounter_type_uuid`
    INTO @encounter_type_uuid
    FROM temp_concept_metadata
    LIMIT 1;

    IF @column_labels IS NOT NULL THEN

        CALL sp_icare_flat_encounter_table_question_concepts_insert(
                p_flat_table_name,
                p_encounter_id,
                @encounter_type_uuid,
                @column_labels
             );

        CALL sp_icare_flat_encounter_table_answer_concepts_insert(
                p_flat_table_name,
                p_encounter_id,
                @encounter_type_uuid,
                @column_labels
             );
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_table_insert_all;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_table_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM icare_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_icare_flat_encounter_table_insert(tbl_name, NULL); -- Insert all OBS/Encounters for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_table_question_concepts_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_table_question_concepts_insert;


-- SP inserts all concepts that are questions or have a concept_id value in the Obs table
-- whether their values/answers are coded or non-coded
~-~-
CREATE PROCEDURE sp_icare_flat_encounter_table_question_concepts_insert(
    IN p_table_name VARCHAR(60),
    IN p_encounter_id INT,
    IN p_encounter_type_uuid CHAR(38),
    IN p_column_labels TEXT
)
BEGIN
    DECLARE sql_stmt TEXT;

    -- Construct base INSERT statement
    SET sql_stmt = CONCAT(
            'INSERT INTO `', p_table_name, '` ',
            'SELECT
                o.encounter_id,
                MAX(o.visit_id) AS visit_id,
                o.person_id,
                o.encounter_datetime,
                MAX(o.location_id) AS location_id,
                ', p_column_labels, '
        FROM icare_z_encounter_obs o
        INNER JOIN temp_concept_metadata tcm
            ON tcm.concept_uuid = o.obs_question_uuid
        WHERE 1=1 ');

    -- Add encounter_id filter if provided
    IF p_encounter_id IS NOT NULL THEN
        SET sql_stmt = CONCAT(sql_stmt,
                              ' AND o.encounter_id = ', p_encounter_id);
    END IF;

    -- Add remaining conditions
    SET sql_stmt = CONCAT(sql_stmt,
                          ' AND o.encounter_type_uuid = ''', p_encounter_type_uuid, '''
          AND tcm.obs_value_column IS NOT NULL
          AND o.obs_group_id IS NULL
          AND o.voided = 0
        GROUP BY o.encounter_id, o.person_id, o.encounter_datetime
        ORDER BY o.encounter_id ASC');

    SET @sql = sql_stmt;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_table_answer_concepts_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_table_answer_concepts_insert;


-- Create a stored procedure to insert answer concepts into a flat table
-- These are concepts that are answers to other question concepts. e.g. multichoice answers in a select or dropdown or radio answers
-- e.g. Key population. They are usually represented as Yes/No or 1/0 or just their concept name under their column name.
-- they dont have a concept_id value or entry in the Obs table, that's why we join on o.obs_value_coded_uuid
~-~-
CREATE PROCEDURE sp_icare_flat_encounter_table_answer_concepts_insert(
    IN p_table_name VARCHAR(60),
    IN p_encounter_id INT,
    IN p_encounter_type_uuid CHAR(38),
    IN p_column_labels TEXT
)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE update_columns TEXT;

    -- Generate UPDATE part for ON DUPLICATE KEY UPDATE
    SELECT GROUP_CONCAT(
                   CONCAT('`', column_label, '` = COALESCE(VALUES(`',
                          column_label, '`), `', column_label, '`)')
           )
    INTO update_columns
    FROM temp_concept_metadata;

    -- Construct base INSERT statement
    SET sql_stmt = CONCAT(
            'INSERT INTO `', p_table_name, '` ',
            'SELECT
                o.encounter_id,
                MAX(o.visit_id) AS visit_id,
                o.person_id,
                o.encounter_datetime,
                MAX(o.location_id) AS location_id,
                ', p_column_labels, '
        FROM icare_z_encounter_obs o
        INNER JOIN temp_concept_metadata tcm
            ON tcm.concept_uuid = o.obs_value_coded_uuid
        WHERE 1=1 '
                   );

    -- Add encounter_id filter if provided
    IF p_encounter_id IS NOT NULL THEN
        SET sql_stmt = CONCAT(sql_stmt,
                              ' AND o.encounter_id = ', p_encounter_id);
    END IF;

    -- Add remaining conditions and ON DUPLICATE KEY UPDATE clause
    SET sql_stmt = CONCAT(sql_stmt,
                          ' AND o.encounter_type_uuid = ''', p_encounter_type_uuid, '''
          AND tcm.obs_value_column IS NOT NULL
          AND o.obs_group_id IS NULL
          AND o.voided = 0
        GROUP BY o.encounter_id, o.person_id, o.encounter_datetime
        ORDER BY o.encounter_id ASC
        ON DUPLICATE KEY UPDATE ', update_columns);

    -- Execute the statement
    SET @sql = sql_stmt;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_incremental_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_incremental_create_all;


~-~-
CREATE PROCEDURE sp_icare_flat_table_incremental_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name)
        FROM icare_concept_metadata md
        WHERE incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_icare_drop_table(tbl_name);
        CALL sp_icare_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_incremental_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_incremental_insert_all;


~-~-
CREATE PROCEDURE sp_icare_flat_table_incremental_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name)
        FROM icare_concept_metadata md
        WHERE incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_icare_flat_encounter_table_insert(tbl_name, NULL); -- Insert all OBS/Encounters for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_incremental_update_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_incremental_update_encounter;


~-~-
CREATE PROCEDURE sp_icare_flat_table_incremental_update_encounter()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE encounter_id INT;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT eo.encounter_id, cm.flat_table_name
        FROM icare_z_encounter_obs eo
                 INNER JOIN icare_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO encounter_id, tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_icare_flat_encounter_table_insert(tbl_name, encounter_id); -- Update only OBS/Encounters that have been modified for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_incremental_update_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_incremental_update_encounter;


~-~-
CREATE PROCEDURE sp_icare_flat_table_incremental_update_encounter()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE encounter_id INT;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT eo.encounter_id, cm.flat_table_name
        FROM icare_z_encounter_obs eo
                 INNER JOIN icare_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO encounter_id, tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_icare_flat_encounter_table_insert(tbl_name, encounter_id); -- Update only OBS/Encounters that have been modified for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_obs_group_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_icare_flat_encounter_obs_group_table_create`;


~-~-
CREATE PROCEDURE `sp_icare_flat_encounter_obs_group_table_create`(
    IN `flat_encounter_table_name` VARCHAR(60) CHARSET UTF8MB4,
    IN `obs_group_concept_name` VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

SET session group_concat_max_len = 20000;
SET @column_labels := NULL;
    SET @tbl_obs_group_name = CONCAT(LEFT(`flat_encounter_table_name`, 50), '_', `obs_group_concept_name`); -- TODO: 50 + 12 to make 62

        SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', @tbl_obs_group_name, '`');

SELECT GROUP_CONCAT(CONCAT(`column_label`, ' ', fn_icare_get_datatype_for_concept(`concept_datatype`)) SEPARATOR ', ')
INTO @column_labels
FROM `icare_concept_metadata` cm
         INNER JOIN
     (SELECT DISTINCT `obs_question_concept_id`
      FROM `icare_z_encounter_obs` eo
               INNER JOIN `icare_obs_group` og
                          ON eo.`obs_id` = og.`obs_id`
      WHERE eo.`obs_group_id` IS NOT NULL
        AND og.`obs_group_concept_name` = `obs_group_concept_name`) eo
     ON cm.`concept_id` = eo.`obs_question_concept_id`
WHERE `flat_table_name` = `flat_encounter_table_name`
  AND `concept_datatype` IS NOT NULL;

IF @column_labels IS NOT NULL THEN
        SET @create_table = CONCAT(
                'CREATE TABLE `', @tbl_obs_group_name, '` (',
                '`encounter_id` INT NOT NULL,',
                '`visit_id` INT NULL,',
                '`client_id` INT NOT NULL,',
                '`encounter_datetime` DATETIME NOT NULL,',
                '`location_id` INT NULL, '
                '`obs_group_id` INT NOT NULL,', @column_labels,

                ',INDEX `icare_idx_encounter_id` (`encounter_id`),',
                'INDEX `icare_idx_visit_id` (`visit_id`),',
                'INDEX `icare_idx_client_id` (`client_id`),',
                'INDEX `icare_idx_encounter_datetime` (`encounter_datetime`),',
                'INDEX `icare_idx_location_id` (`location_id`));'
        );
END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE deletetb FROM @drop_table;
PREPARE createtb FROM @create_table;

EXECUTE deletetb;
EXECUTE createtb;

DEALLOCATE PREPARE deletetb;
DEALLOCATE PREPARE createtb;
END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_obs_group_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_obs_group_table_create_all;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_obs_group_table_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE obs_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT 0;

    DECLARE cursor_flat_tables CURSOR FOR
    SELECT DISTINCT(flat_table_name) FROM icare_concept_metadata;

    DECLARE cursor_obs_group_tables CURSOR FOR
    SELECT DISTINCT(obs_group_concept_name) FROM icare_obs_group;

    -- DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cursor_flat_tables;

        REPEAT
            FETCH cursor_flat_tables INTO tbl_name;
                IF NOT done THEN
                    OPEN cursor_obs_group_tables;
                        block2: BEGIN
                            DECLARE doneobs_name INT DEFAULT 0;
                            DECLARE firstobs_name varchar(255) DEFAULT '';
                            DECLARE i int DEFAULT 1;
                            DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneobs_name = 1;

                            REPEAT
                                FETCH cursor_obs_group_tables INTO obs_name;

                                    IF i = 1 THEN
                                        SET firstobs_name = obs_name;
                                    END IF;

                                    CALL sp_icare_flat_encounter_obs_group_table_create(tbl_name,obs_name);
                                    SET i = i + 1;

                                UNTIL doneobs_name
                            END REPEAT;

                            CALL sp_icare_flat_encounter_obs_group_table_create(tbl_name,firstobs_name);
                        END block2;
                    CLOSE cursor_obs_group_tables;
                END IF;
            UNTIL done
        END REPEAT;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_obs_group_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_obs_group_table_insert;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_obs_group_table_insert(
    IN flat_encounter_table_name VARCHAR(60) CHARACTER SET UTF8MB4,
    IN obs_group_concept_name VARCHAR(255) CHARACTER SET UTF8MB4,
    IN encounter_id INT -- Optional parameter for incremental insert
)
BEGIN
    -- Set maximum length for GROUP_CONCAT
SET session group_concat_max_len = 20000;

-- Set up table name and encounter_id variables
SET @tbl_name = flat_encounter_table_name;
    SET @obs_group_name = obs_group_concept_name;
    SET @enc_id = encounter_id;

    -- Generate observation group table name dynamically
    SET @tbl_obs_group_name = CONCAT(LEFT(@tbl_name, 50), '_', obs_group_concept_name);

    -- Handle the optional encounter_id parameter
    IF @enc_id IS NOT NULL THEN
        -- If encounter_id is provided, delete existing records for that encounter_id
        SET @delete_stmt = CONCAT('DELETE FROM `', @tbl_obs_group_name, '` WHERE `encounter_id` = ', @enc_id);
PREPARE deletetbl FROM @delete_stmt;
EXECUTE deletetbl;
DEALLOCATE PREPARE deletetbl;
ELSE
        SET @enc_id = 0;
END IF;

    -- Create and populate a temporary table for concept metadata
    CREATE TEMPORARY TABLE IF NOT EXISTS `icare_temp_concept_metadata_group` (
        `id` INT NOT NULL,
        `flat_table_name` VARCHAR(60) NOT NULL,
        `encounter_type_uuid` CHAR(38) NOT NULL,
        `column_label` VARCHAR(255) NOT NULL,
        `concept_uuid` CHAR(38) NOT NULL,
        `obs_value_column` VARCHAR(50),
        `concept_answer_obs` INT,
        INDEX (`id`),
        INDEX (`column_label`),
        INDEX (`concept_uuid`),
        INDEX (`concept_answer_obs`),
        INDEX (`flat_table_name`),
        INDEX (`encounter_type_uuid`)
    ) CHARSET = UTF8MB4;

TRUNCATE TABLE `icare_temp_concept_metadata_group`;

INSERT INTO `icare_temp_concept_metadata_group`
SELECT DISTINCT
    cm.`id`,
    cm.`flat_table_name`,
    cm.`encounter_type_uuid`,
    cm.`column_label`,
    cm.`concept_uuid`,
    fn_icare_get_obs_value_column(cm.`concept_datatype`) AS `obs_value_column`,
    cm.`concept_answer_obs`
FROM `icare_concept_metadata` cm
         INNER JOIN (
    SELECT DISTINCT eo.`obs_question_concept_id`
    FROM `icare_z_encounter_obs` eo
             INNER JOIN `icare_obs_group` og ON eo.`obs_id` = og.`obs_id`
    WHERE og.`obs_group_concept_name` = @obs_group_name
) eo ON cm.`concept_id` = eo.`obs_question_concept_id`
WHERE cm.`flat_table_name` = @tbl_name;

-- Generate dynamic column labels for the insert statement
SELECT GROUP_CONCAT(DISTINCT
                            CONCAT('MAX(CASE WHEN `column_label` = ''', `column_label`, ''' THEN ',
                                   `obs_value_column`, ' END) `', `column_label`, '`')
                            ORDER BY `id` ASC)
INTO @column_labels
FROM `icare_temp_concept_metadata_group`;

SELECT DISTINCT `encounter_type_uuid` INTO @tbl_encounter_type_uuid FROM `icare_temp_concept_metadata_group`;

-- Check if column labels are generated
IF @column_labels IS NOT NULL THEN

    SET @insert_stmt = CONCAT(
            'INSERT INTO `', @tbl_obs_group_name, '` ',
            'SELECT eo.`encounter_id`, MAX(eo.`visit_id`) AS `visit_id`, eo.`person_id`, eo.`encounter_datetime`, MAX(eo.`location_id`) AS `location_id`, eo.`obs_group_id`, ',
            @column_labels, ' ',
            'FROM `icare_z_encounter_obs` eo ',
            'INNER JOIN `icare_temp_concept_metadata_group` tcm ON tcm.`concept_uuid` = eo.`obs_question_uuid` ',
            'WHERE eo.`obs_group_id` IS NOT NULL ',
            'AND eo.`voided` = 0 ',
            IF(@enc_id <> 0, CONCAT('AND eo.`encounter_id` = ', @enc_id, ' '), ''),
            'GROUP BY eo.`encounter_id`, eo.`person_id`, eo.`encounter_datetime`, eo.`obs_group_id` '
        );

PREPARE inserttbl FROM @insert_stmt;
EXECUTE inserttbl;
DEALLOCATE PREPARE inserttbl;

SET @update_stmt = (
            SELECT GROUP_CONCAT(
                CONCAT('`', `column_label`, '` = COALESCE(VALUES(`', `column_label`, '`), `', `column_label`, '`)')
            )
            FROM `icare_temp_concept_metadata_group`
        );

        SET @insert_stmt = CONCAT(
            'INSERT INTO `', @tbl_obs_group_name, '` ',
            'SELECT eo.`encounter_id`, MAX(eo.`visit_id`) AS `visit_id`, eo.`person_id`, eo.`encounter_datetime`, MAX(eo.`location_id`) AS `location_id`,eo.`obs_group_id` , ',
            @column_labels, ' ',
            'FROM `icare_z_encounter_obs` eo ',
            'INNER JOIN `icare_temp_concept_metadata_group` tcm ON tcm.`concept_uuid` = eo.`obs_value_coded_uuid` ',
            'WHERE eo.`obs_group_id` IS NOT NULL ',
            'AND eo.`voided` = 0 ',
            IF(@enc_id <> 0, CONCAT('AND eo.`encounter_id` = ', @enc_id, ' '), ''),
            'GROUP BY eo.`encounter_id`, eo.`person_id`, eo.`encounter_datetime`, eo.`obs_group_id` ',
            'ON DUPLICATE KEY UPDATE ', @update_stmt
        );

PREPARE inserttbl FROM @insert_stmt;
EXECUTE inserttbl;
DEALLOCATE PREPARE inserttbl;
END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_encounter_obs_group_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_icare_flat_encounter_obs_group_table_insert_all;


~-~-
CREATE PROCEDURE sp_icare_flat_encounter_obs_group_table_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE obs_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT 0;

    DECLARE cursor_flat_tables CURSOR FOR
    SELECT DISTINCT(flat_table_name) FROM icare_concept_metadata;

    DECLARE cursor_obs_group_tables CURSOR FOR
    SELECT DISTINCT(obs_group_concept_name) FROM icare_obs_group;

    -- DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cursor_flat_tables;

        REPEAT
            FETCH cursor_flat_tables INTO tbl_name;
                IF NOT done THEN
                    OPEN cursor_obs_group_tables;
                        block2: BEGIN
                            DECLARE doneobs_name INT DEFAULT 0;
                            DECLARE firstobs_name varchar(255) DEFAULT '';
                            DECLARE i int DEFAULT 1;
                            DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneobs_name = 1;

                            REPEAT
                                FETCH cursor_obs_group_tables INTO obs_name;

                                    IF i = 1 THEN
                                        SET firstobs_name = obs_name;
                                    END IF;

                                    CALL sp_icare_flat_encounter_obs_group_table_insert(tbl_name,obs_name,NULL);
                                    SET i = i + 1;

                                UNTIL doneobs_name
                            END REPEAT;

                            CALL sp_icare_flat_encounter_obs_group_table_insert(tbl_name,firstobs_name,NULL);
                        END block2;
                    CLOSE cursor_obs_group_tables;
            END IF;
                        UNTIL done
        END REPEAT;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_multiselect_values_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_icare_multiselect_values_update`;


~-~-
CREATE PROCEDURE `sp_icare_multiselect_values_update`(
    IN table_to_update CHAR(100) CHARACTER SET UTF8MB4,
    IN column_names TEXT CHARACTER SET UTF8MB4,
    IN value_yes CHAR(100) CHARACTER SET UTF8MB4,
    IN value_no CHAR(100) CHARACTER SET UTF8MB4
)
BEGIN

    SET @table_columns = column_names;
    SET @start_pos = 1;
    SET @comma_pos = locate(',', @table_columns);
    SET @end_loop = 0;

    SET @column_label = '';

    REPEAT
        IF @comma_pos > 0 THEN
            SET @column_label = substring(@table_columns, @start_pos, @comma_pos - @start_pos);
            SET @end_loop = 0;
        ELSE
            SET @column_label = substring(@table_columns, @start_pos);
            SET @end_loop = 1;
        END IF;

        -- UPDATE fact_hts SET @column_label=IF(@column_label IS NULL OR '', new_value_if_false, new_value_if_true);

        SET @update_sql = CONCAT(
                'UPDATE ', table_to_update, ' SET ', @column_label, '= IF(', @column_label, ' IS NOT NULL, ''',
                value_yes, ''', ''', value_no, ''');');
        PREPARE stmt FROM @update_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF @end_loop = 0 THEN
            SET @table_columns = substring(@table_columns, @comma_pos + 1);
            SET @comma_pos = locate(',', @table_columns);
        END IF;
    UNTIL @end_loop = 1
        END REPEAT;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_load_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_load_agegroup;


~-~-
CREATE PROCEDURE sp_icare_load_agegroup()
BEGIN
    DECLARE age INT DEFAULT 0;
    WHILE age <= 120
        DO
            INSERT INTO icare_dim_agegroup(age, datim_agegroup, normal_agegroup)
            VALUES (age, fn_icare_calculate_agegroup(age), IF(age < 15, '<15', '15+'));
            SET age = age + 1;
        END WHILE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_write_automated_json_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_write_automated_json_config;


~-~-
CREATE PROCEDURE sp_icare_write_automated_json_config()
BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE jsonData JSON;
    DECLARE cur CURSOR FOR
        SELECT table_json_data FROM icare_flat_table_config;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

        SET @report_data = '{"flat_report_metadata":[';

        OPEN cur;
        FETCH cur INTO jsonData;

        IF NOT done THEN
                    SET @report_data = CONCAT(@report_data, jsonData);
        FETCH cur INTO jsonData; -- Fetch next record after the first one
        END IF;

                read_loop: LOOP
                    IF done THEN
                        LEAVE read_loop;
        END IF;

                    SET @report_data = CONCAT(@report_data, ',', jsonData);
        FETCH cur INTO jsonData;
        END LOOP;
        CLOSE cur;

        SET @report_data = CONCAT(@report_data, ']}');

        CALL sp_icare_extract_report_metadata(@report_data, 'icare_concept_metadata');

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_locale_insert_helper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_locale_insert_helper;


~-~-
CREATE PROCEDURE sp_icare_locale_insert_helper(
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4
)
BEGIN

    SET @conc_locale = concepts_locale;
    SET @insert_stmt = CONCAT('INSERT INTO icare_dim_locale (locale) VALUES (''', @conc_locale, ''');');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_extract_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_extract_report_column_names;


~-~-
CREATE PROCEDURE sp_icare_extract_report_column_names()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE proc_name VARCHAR(255);
    DECLARE cur CURSOR FOR SELECT DISTINCT report_columns_procedure_name FROM icare_dim_report_definition;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO proc_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Fetch the parameters for the procedure and provide empty string values for each
        SET @params := NULL;

        SELECT GROUP_CONCAT('\'\'' SEPARATOR ', ')
        INTO @params
        FROM icare_dim_report_definition_parameters rdp
                 INNER JOIN icare_dim_report_definition rd on rdp.report_id = rd.report_id
        WHERE rd.report_columns_procedure_name = proc_name;

        IF @params IS NULL THEN
            SET @procedure_call = CONCAT('CALL ', proc_name, '();');
        ELSE
            SET @procedure_call = CONCAT('CALL ', proc_name, '(', @params, ');');
        END IF;

        PREPARE stmt FROM @procedure_call;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_extract_report_definition_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_extract_report_definition_metadata;


~-~-
CREATE PROCEDURE sp_icare_extract_report_definition_metadata(
    IN report_definition_json JSON,
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    IF report_definition_json IS NULL OR JSON_LENGTH(report_definition_json) = 0 THEN
        SIGNAL SQLSTATE '02000'
            SET MESSAGE_TEXT = 'Warn: report_definition_json is empty or null.';
    ELSE

        SET session group_concat_max_len = 20000;

        SELECT JSON_EXTRACT(report_definition_json, '$.report_definitions') INTO @report_array;
        SELECT JSON_LENGTH(@report_array) INTO @report_array_len;

        SET @report_count = 0;
        WHILE @report_count < @report_array_len
            DO

                SELECT JSON_EXTRACT(@report_array, CONCAT('$[', @report_count, ']')) INTO @report;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, '$.report_name')) INTO @report_name;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, '$.report_id')) INTO @report_id;

                SELECT CONCAT('sp_icare_report_', @report_id, '_query') INTO @report_procedure_name;
                SELECT CONCAT('sp_icare_report_', @report_id, '_columns_query') INTO @report_columns_procedure_name;
                SELECT CONCAT('sp_icare_report_', @report_id, '_size_query') INTO @report_size_procedure_name;
                SELECT CONCAT('icare_report_', @report_id) INTO @table_name;

                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, CONCAT('$.report_sql.sql_query'))) INTO @sql_query;
                SELECT JSON_EXTRACT(@report, CONCAT('$.report_sql.query_params')) INTO @query_params_array;
                SELECT JSON_EXTRACT(@report, CONCAT('$.report_sql.paginate')) INTO @paginate_flag;

                INSERT INTO icare_dim_report_definition(report_id,
                                                        report_procedure_name,
                                                        report_columns_procedure_name,
                                                        report_size_procedure_name,
                                                        sql_query,
                                                        table_name,
                                                        report_name)
                VALUES (@report_id,
                        @report_procedure_name,
                        @report_columns_procedure_name,
                        @report_size_procedure_name,
                        @sql_query,
                        @table_name,
                        @report_name);

                -- Iterate over the "params" array for each report
                SELECT JSON_LENGTH(@query_params_array) INTO @total_params;

                SET @parameters := NULL;
                SET @param_count = 0;
                WHILE @param_count < @total_params
                    DO
                        SELECT JSON_EXTRACT(@query_params_array, CONCAT('$[', @param_count, ']')) INTO @param;
                        SELECT JSON_UNQUOTE(JSON_EXTRACT(@param, '$.name')) INTO @param_name;
                        SELECT JSON_UNQUOTE(JSON_EXTRACT(@param, '$.type')) INTO @param_type;
                        SET @param_position = @param_count + 1;

                        INSERT INTO icare_dim_report_definition_parameters(report_id,
                                                                           parameter_name,
                                                                           parameter_type,
                                                                           parameter_position)
                        VALUES (@report_id,
                                @param_name,
                                @param_type,
                                @param_position);

                        SET @param_count = @param_position;
                    END WHILE;

                -- Handle pagination parameters if paginate flag is true
                IF @paginate_flag = TRUE OR @paginate_flag = 'true' THEN
                    -- Add page_number parameter
                    SET @page_number_position = @total_params + 1;
                    INSERT INTO icare_dim_report_definition_parameters(report_id,
                                                                      parameter_name,
                                                                      parameter_type,
                                                                      parameter_position)
                    VALUES (@report_id,
                            'page_number',
                            'INT',
                            @page_number_position);
                            
                    -- Add page_size parameter
                    SET @page_size_position = @total_params + 2;
                    INSERT INTO icare_dim_report_definition_parameters(report_id,
                                                                      parameter_name,
                                                                      parameter_type,
                                                                      parameter_position)
                    VALUES (@report_id,
                            'page_size',
                            'INT',
                            @page_size_position);
                END IF;

                SET @report_count = @report_count + 1;
            END WHILE;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_generate_report_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_generate_report_wrapper;


~-~-
CREATE PROCEDURE sp_icare_generate_report_wrapper(IN generate_columns_flag TINYINT(1),
                                                  IN report_identifier VARCHAR(255),
                                                  IN parameter_list JSON)
BEGIN

    DECLARE proc_name VARCHAR(255);
    DECLARE sql_args VARCHAR(1000);
    DECLARE arg_name VARCHAR(50);
    DECLARE arg_value VARCHAR(255);
    DECLARE tester VARCHAR(255);
    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_parameter_names CURSOR FOR
        SELECT DISTINCT (p.parameter_name)
        FROM icare_dim_report_definition_parameters p
        WHERE p.report_id = report_identifier;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    IF generate_columns_flag = 1 THEN
        SET proc_name = (SELECT DISTINCT (rd.report_columns_procedure_name)
                         FROM icare_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);
    ELSE
        SET proc_name = (SELECT DISTINCT (rd.report_procedure_name)
                         FROM icare_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);
    END IF;

    OPEN cursor_parameter_names;
    read_loop:
    LOOP
        FETCH cursor_parameter_names INTO arg_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        SET arg_value = IFNULL((JSON_EXTRACT(parameter_list, CONCAT('$[', ((SELECT p.parameter_position
                                                                            FROM icare_dim_report_definition_parameters p
                                                                            WHERE p.parameter_name = arg_name
                                                                              AND p.report_id = report_identifier) - 1),
                                                                    '].value'))), 'NULL');
        SET tester = CONCAT_WS(', ', tester, arg_value);
        SET sql_args = IFNULL(CONCAT_WS(', ', sql_args, arg_value), NULL);

    END LOOP;

    CLOSE cursor_parameter_names;

    SET @sql = CONCAT('CALL ', proc_name, '(', IFNULL(sql_args, ''), ')');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_generate_report_size_sp_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_generate_report_size_sp_wrapper;


~-~-
CREATE PROCEDURE sp_icare_generate_report_size_sp_wrapper(
    IN report_identifier VARCHAR(255),
    IN parameter_list JSON)
BEGIN

    DECLARE proc_name VARCHAR(255);
    DECLARE sql_args VARCHAR(1000);
    DECLARE arg_name VARCHAR(50);
    DECLARE arg_value VARCHAR(255);
    DECLARE tester VARCHAR(255);
    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_parameter_names CURSOR FOR
        SELECT DISTINCT (p.parameter_name)
        FROM icare_dim_report_definition_parameters p
        WHERE p.report_id = report_identifier
        AND p.parameter_name NOT IN ('page_number', 'page_size');

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET proc_name = (SELECT DISTINCT (rd.report_size_procedure_name)
                         FROM icare_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);

    OPEN cursor_parameter_names;
    read_loop:
    LOOP
        FETCH cursor_parameter_names INTO arg_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        SET arg_value = IFNULL((JSON_EXTRACT(parameter_list, CONCAT('$[', ((SELECT p.parameter_position
                                                                            FROM icare_dim_report_definition_parameters p
                                                                            WHERE p.parameter_name = arg_name
                                                                              AND p.report_id = report_identifier) - 1),
                                                                    '].value'))), 'NULL');
        SET tester = CONCAT_WS(', ', tester, arg_value);
        SET sql_args = IFNULL(CONCAT_WS(', ', sql_args, arg_value), NULL);

    END LOOP;

    CLOSE cursor_parameter_names;

    SET @sql = CONCAT('CALL ', proc_name, '(', IFNULL(sql_args, ''), ')');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_get_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_get_report_column_names;


~-~-
CREATE PROCEDURE sp_icare_get_report_column_names(IN report_identifier VARCHAR(255))
BEGIN

    -- We could also pick the column names from the report definition table but it is in a comma-separated list (weigh both options)
    SELECT table_name
    INTO @table_name
    FROM icare_dim_report_definition
    WHERE report_id = report_identifier;

    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @table_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_reset_incremental_update_flag  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Given a table name, this procedure will reset the incremental_record column to 0 for all rows where the incremental_record is 1.
-- This is useful when we want to re-run the incremental updates for a table.

DROP PROCEDURE IF EXISTS sp_icare_reset_incremental_update_flag;


~-~-
CREATE PROCEDURE sp_icare_reset_incremental_update_flag(
    IN table_name VARCHAR(60) CHARACTER SET UTF8MB4
)
BEGIN

    SET @tbl_name = table_name;

    SET @update_stmt =
            CONCAT('UPDATE ', @tbl_name, ' SET incremental_record = 0 WHERE incremental_record = 1');
    PREPARE updatetb FROM @update_stmt;
    EXECUTE updatetb;
    DEALLOCATE PREPARE updatetb;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_reset_incremental_update_flag_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_reset_incremental_update_flag_all;


~-~-
CREATE PROCEDURE sp_icare_reset_incremental_update_flag_all()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_reset_incremental_update_flag_all', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_reset_incremental_update_flag_all', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Given a table name, this procedure will reset the incremental_record column to 0 for all rows where the incremental_record is 1.
-- This is useful when we want to re-run the incremental updates for a table.

CALL sp_icare_reset_incremental_update_flag('icare_dim_location');
CALL sp_icare_reset_incremental_update_flag('icare_dim_patient_identifier_type');
CALL sp_icare_reset_incremental_update_flag('icare_dim_concept_datatype');
CALL sp_icare_reset_incremental_update_flag('icare_dim_concept_name');
CALL sp_icare_reset_incremental_update_flag('icare_dim_concept');
CALL sp_icare_reset_incremental_update_flag('icare_dim_concept_answer');
CALL sp_icare_reset_incremental_update_flag('icare_dim_encounter_type');
CALL sp_icare_reset_incremental_update_flag('icare_flat_table_config');
CALL sp_icare_reset_incremental_update_flag('icare_concept_metadata');
CALL sp_icare_reset_incremental_update_flag('icare_dim_encounter');
CALL sp_icare_reset_incremental_update_flag('icare_dim_person_name');
CALL sp_icare_reset_incremental_update_flag('icare_dim_person');
CALL sp_icare_reset_incremental_update_flag('icare_dim_person_attribute_type');
CALL sp_icare_reset_incremental_update_flag('icare_dim_person_attribute');
CALL sp_icare_reset_incremental_update_flag('icare_dim_person_address');
CALL sp_icare_reset_incremental_update_flag('icare_dim_users');
CALL sp_icare_reset_incremental_update_flag('icare_dim_relationship');
CALL sp_icare_reset_incremental_update_flag('icare_dim_patient_identifier');
CALL sp_icare_reset_incremental_update_flag('icare_dim_orders');
CALL sp_icare_reset_incremental_update_flag('icare_z_encounter_obs');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_concept_metadata_create();
CALL sp_icare_concept_metadata_insert();
CALL sp_icare_concept_metadata_missing_columns_insert(); -- Update/insert table column metadata configs without table_columns json
CALL sp_icare_concept_metadata_update();
CALL sp_icare_concept_metadata_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_create;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_concept_metadata
(
    id                  INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    concept_id          INT          NULL,
    concept_uuid        CHAR(38)     NOT NULL,
    concept_name        VARCHAR(255) NULL,
    column_number       INT,
    column_label        VARCHAR(60)  NOT NULL,
    concept_datatype    VARCHAR(255) NULL,
    concept_answer_obs  TINYINT(1)   NOT NULL DEFAULT 0,
    report_name         VARCHAR(255) NOT NULL,
    flat_table_name     VARCHAR(60)  NULL,
    encounter_type_uuid CHAR(38)     NOT NULL,
    row_num             INT          NULL     DEFAULT 1,
    incremental_record  INT          NOT NULL DEFAULT 0,

    INDEX icare_idx_concept_id (concept_id),
    INDEX icare_idx_concept_uuid (concept_uuid),
    INDEX icare_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX icare_idx_row_num (row_num),
    INDEX icare_idx_concept_datatype (concept_datatype),
    INDEX icare_idx_flat_table_name (flat_table_name),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_insert;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @is_incremental = 0;
-- SET @report_data = fn_icare_generate_json_from_icare_flat_table_config(@is_incremental);
CALL sp_icare_concept_metadata_insert_helper(@is_incremental, 'icare_concept_metadata');


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_insert_helper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_insert_helper;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_insert_helper(
    IN is_incremental TINYINT(1),
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    DECLARE is_incremental_record TINYINT(1) DEFAULT 0;
    DECLARE report_json JSON;
    DECLARE done INT DEFAULT FALSE;
    DECLARE cur CURSOR FOR
        -- selects rows where incremental_record is 1. If is_incremental is not 1, it selects all rows.
        SELECT table_json_data
        FROM icare_flat_table_config
        WHERE (IF(is_incremental = 1, incremental_record = 1, 1));

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET session group_concat_max_len = 20000;

    SELECT DISTINCT(table_partition_number)
    INTO @table_partition_number
    FROM _icare_etl_user_settings;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO report_json;

        IF done THEN
            LEAVE read_loop;
        END IF;

        SELECT JSON_EXTRACT(report_json, '$.report_name') INTO @report_name;
        SELECT JSON_EXTRACT(report_json, '$.flat_table_name') INTO @flat_table_name;
        SELECT JSON_EXTRACT(report_json, '$.encounter_type_uuid') INTO @encounter_type;
        SELECT JSON_EXTRACT(report_json, '$.table_columns') INTO @column_array;

        SELECT JSON_KEYS(@column_array) INTO @column_keys_array;
        SELECT JSON_LENGTH(@column_keys_array) INTO @column_keys_array_len;

        -- if is_incremental = 1, delete records (if they exist) from icare_concept_metadata table with encounter_type_uuid = @encounter_type
        IF is_incremental = 1 THEN

            SET is_incremental_record = 1;
            SET @delete_query = CONCAT('DELETE FROM icare_concept_metadata WHERE encounter_type_uuid = ''',
                                       JSON_UNQUOTE(@encounter_type), '''');

            PREPARE stmt FROM @delete_query;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        IF @column_keys_array_len = 0 THEN

            INSERT INTO icare_concept_metadata
            (report_name,
             flat_table_name,
             encounter_type_uuid,
             column_label,
             concept_uuid,
             incremental_record)
            VALUES (JSON_UNQUOTE(@report_name),
                    JSON_UNQUOTE(@flat_table_name),
                    JSON_UNQUOTE(@encounter_type),
                    'AUTO-GENERATE',
                    'AUTO-GENERATE',
                    is_incremental_record);
        ELSE

            SET @col_count = 0;
            SET @table_name = JSON_UNQUOTE(@flat_table_name);


            WHILE @col_count < @column_keys_array_len
                DO
                    SELECT JSON_EXTRACT(@column_keys_array, CONCAT('$[', @col_count, ']')) INTO @field_name;
                    SELECT JSON_EXTRACT(@column_array, CONCAT('$.', @field_name)) INTO @concept_uuid;

                    IF @col_count < @table_partition_number THEN
                        SET @table_name = @table_name;
                    ELSE
                        SET @table_name = CONCAT(LEFT(JSON_UNQUOTE(@flat_table_name), 57), '_', FLOOR((@col_count - @table_partition_number) / @table_partition_number)+1);
                    END IF;

                    INSERT INTO icare_concept_metadata
                    (report_name,
                     flat_table_name,
                     encounter_type_uuid,
                     column_label,
                     concept_uuid,
                     incremental_record)
                    VALUES (JSON_UNQUOTE(@report_name),
                            JSON_UNQUOTE(@table_name),
                            JSON_UNQUOTE(@encounter_type),
                            JSON_UNQUOTE(@field_name),
                            JSON_UNQUOTE(@concept_uuid),
                            is_incremental_record);
                    SET @col_count = @col_count + 1;
                END WHILE;
        END IF;
    END LOOP;

    CLOSE cur;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_update;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE icare_concept_metadata md
    INNER JOIN icare_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = c.name
WHERE md.id > 0;

-- All Records' concept_answer_obs field is set to 0 by default
-- what will remain with (concept_answer_obs = 0) after the 2 updates
-- are Question concepts that have other values other than concepts as answers

-- First update: Get All records that are answer concepts (Answers to other question concepts)
-- concept_answer_obs = 1
UPDATE icare_concept_metadata md
    INNER JOIN icare_dim_concept_answer answer
    ON md.concept_id = answer.answer_concept
SET md.concept_answer_obs = 1
WHERE NOT EXISTS (SELECT 1
                  FROM icare_dim_concept_answer question
                  WHERE question.concept_id = answer.answer_concept);

-- Second update: Get All records that are Both a Question concept and an Answer concept
-- concept_answer_obs = 2
UPDATE icare_concept_metadata md
    INNER JOIN icare_dim_concept_answer answer
    ON md.concept_id = answer.concept_id
SET md.concept_answer_obs = 2
WHERE EXISTS (SELECT 1
              FROM icare_dim_concept_answer answer2
              WHERE answer2.answer_concept = answer.concept_id);

-- Update row number
SET @row_number = 0;
SET @prev_flat_table_name = NULL;
SET @prev_concept_id = NULL;

UPDATE icare_concept_metadata md
    INNER JOIN (SELECT flat_table_name,
                       concept_id,
                       id,
                       @row_number := CASE
                                          WHEN @prev_flat_table_name = flat_table_name
                                              AND @prev_concept_id = concept_id
                                              THEN @row_number + 1
                                          ELSE 1
                           END AS num,
                       @prev_flat_table_name := flat_table_name,
                       @prev_concept_id := concept_id
                FROM icare_concept_metadata
                ORDER BY flat_table_name, concept_id, id) m ON md.id = m.id
SET md.row_num = num
WHERE md.id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_cleanup;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- delete un wanted rows after inserting columns that were not given in the .json config file into the meta data table,
-- all rows with 'AUTO-GENERATE' are not used anymore. Delete them/1
DELETE
FROM icare_concept_metadata
WHERE concept_uuid = 'AUTO-GENERATE'
  AND column_label = 'AUTO-GENERATE';

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_missing_columns_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_missing_columns_insert;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_missing_columns_insert()
BEGIN

    DECLARE encounter_type_uuid_value CHAR(38);
    DECLARE report_name_val VARCHAR(100);
    DECLARE encounter_type_id_val INT;
    DECLARE flat_table_name_val VARCHAR(255);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounters CURSOR FOR
        SELECT DISTINCT(encounter_type_uuid), m.report_name, m.flat_table_name, et.encounter_type_id
        FROM icare_concept_metadata m
                 INNER JOIN icare_dim_encounter_type et ON m.encounter_type_uuid = et.uuid
        WHERE et.retired = 0
          AND m.concept_uuid = 'AUTO-GENERATE'
          AND m.column_label = 'AUTO-GENERATE';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_encounters;
    computations_loop:
    LOOP
        FETCH cursor_encounters
            INTO encounter_type_uuid_value, report_name_val, flat_table_name_val, encounter_type_id_val;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO icare_concept_metadata
                (
                    report_name,
                    flat_table_name,
                    encounter_type_uuid,
                    column_label,
                    concept_uuid
                )
                SELECT
                    ''', report_name_val, ''',
                ''', flat_table_name_val, ''',
                ''', encounter_type_uuid_value, ''',
                field_name,
                concept_uuid,
                FROM (
                     SELECT
                          DISTINCT et.encounter_type_id,
                          c.auto_table_column_name AS field_name,
                          c.uuid AS concept_uuid
                     FROM icare_source_db.obs o
                          INNER JOIN icare_source_db.encounter e
                            ON e.encounter_id = o.encounter_id
                          INNER JOIN icare_dim_encounter_type et
                            ON e.encounter_type = et.encounter_type_id
                          INNER JOIN icare_dim_concept c
                            ON o.concept_id = c.concept_id
                     WHERE et.encounter_type_id = ''', encounter_type_id_val, '''
                       AND et.retired = 0
                ) icare_missing_concept;
            ');

        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

    END LOOP computations_loop;
    CLOSE cursor_encounters;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_incremental;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_concept_metadata_incremental_insert();
CALL sp_icare_concept_metadata_missing_columns_incremental_insert(); -- Update/insert table column metadata configs without table_columns json
CALL sp_icare_concept_metadata_incremental_update();
CALL sp_icare_concept_metadata_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @is_incremental = 1;
-- SET @report_data = fn_icare_generate_json_from_icare_flat_table_config(@is_incremental);
CALL sp_icare_concept_metadata_insert_helper(@is_incremental, 'icare_concept_metadata');


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE icare_concept_metadata md
    INNER JOIN icare_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = c.name
WHERE md.incremental_record = 1;

-- Update to True if this field is an obs answer to an obs Question
UPDATE icare_concept_metadata md
    INNER JOIN icare_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.incremental_record = 1
  AND md.concept_id IN (SELECT DISTINCT ca.concept_id
                        FROM icare_dim_concept_answer ca);

-- Update to for multiple selects/dropdowns/options this field is an obs answer to an obs Question
-- TODO: check this implementation here
UPDATE icare_concept_metadata md
SET md.concept_answer_obs = 1
WHERE md.incremental_record = 1
  and concept_datatype = 'N/A';

-- Update row number
SET @row_number = 0;
SET @prev_flat_table_name = NULL;
SET @prev_concept_id = NULL;

UPDATE icare_concept_metadata md
    INNER JOIN (SELECT flat_table_name,
                       concept_id,
                       id,
                       @row_number := CASE
                                          WHEN @prev_flat_table_name = flat_table_name
                                              AND @prev_concept_id = concept_id
                                              THEN @row_number + 1
                                          ELSE 1
                           END AS num,
                       @prev_flat_table_name := flat_table_name,
                       @prev_concept_id := concept_id
                FROM icare_concept_metadata
                -- WHERE incremental_record = 1
                ORDER BY flat_table_name, concept_id, id) m ON md.id = m.id
SET md.row_num = num
WHERE md.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_incremental_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_concept_metadata_incremental_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_concept_metadata_incremental_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- delete un wanted rows after inserting columns that were not given in the .json config file into the meta data table,
-- all rows with 'AUTO-GENERATE' are not used anymore. Delete them/1
DELETE
FROM icare_concept_metadata
WHERE incremental_record = 1
  AND concept_uuid = 'AUTO-GENERATE'
  AND column_label = 'AUTO-GENERATE';

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_concept_metadata_missing_columns_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_concept_metadata_missing_columns_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_concept_metadata_missing_columns_incremental_insert()
BEGIN

    DECLARE encounter_type_uuid_value CHAR(38);
    DECLARE report_name_val VARCHAR(100);
    DECLARE encounter_type_id_val INT;
    DECLARE flat_table_name_val VARCHAR(255);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounters CURSOR FOR
        SELECT DISTINCT(encounter_type_uuid), m.report_name, m.flat_table_name, et.encounter_type_id
        FROM icare_concept_metadata m
                 INNER JOIN icare_dim_encounter_type et ON m.encounter_type_uuid = et.uuid
        WHERE et.retired = 0
          AND m.concept_uuid = 'AUTO-GENERATE'
          AND m.column_label = 'AUTO-GENERATE'
          AND m.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_encounters;
    computations_loop:
    LOOP
        FETCH cursor_encounters
            INTO encounter_type_uuid_value, report_name_val, flat_table_name_val, encounter_type_id_val;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO icare_concept_metadata
                (
                    report_name,
                    flat_table_name,
                    encounter_type_uuid,
                    column_label,
                    concept_uuid,
                    incremental_record
                )
                SELECT
                    ''', report_name_val, ''',
                ''', flat_table_name_val, ''',
                ''', encounter_type_uuid_value, ''',
                field_name,
                concept_uuid,
                1
                FROM (
                     SELECT
                          DISTINCT et.encounter_type_id,
                          c.auto_table_column_name AS field_name,
                          c.uuid AS concept_uuid
                     FROM icare_source_db.obs o
                          INNER JOIN icare_source_db.encounter e
                            ON e.encounter_id = o.encounter_id
                          INNER JOIN icare_dim_encounter_type et
                            ON e.encounter_type = et.encounter_type_id
                          INNER JOIN icare_dim_concept c
                            ON o.concept_id = c.concept_id
                     WHERE et.encounter_type_id = ''', encounter_type_id_val, '''
                       AND et.retired = 0
                ) icare_missing_concept;
            ');

        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

    END LOOP computations_loop;
    CLOSE cursor_encounters;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_create;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_flat_table_config
(
    id                   INT           NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    encounter_type_id    INT           NOT NULL UNIQUE,
    report_name          VARCHAR(100)  NOT NULL,
    table_json_data      JSON          NOT NULL,
    table_json_data_hash CHAR(32)      NULL,
    encounter_type_uuid  CHAR(38)      NOT NULL,
    incremental_record   INT DEFAULT 0 NOT NULL COMMENT 'Whether `table_json_data` has been modified or not',

    INDEX icare_idx_encounter_type_id (encounter_type_id),
    INDEX icare_idx_report_name (report_name),
    INDEX icare_idx_table_json_data_hash (table_json_data_hash),
    INDEX icare_idx_uuid (encounter_type_uuid),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_insert_helper_manual  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- manually extracts user given flat table config file json into the icare_flat_table_config table
-- this data together with automatically extracted flat table data is inserted into the icare_flat_table_config table
-- later it is processed by the 'fn_icare_generate_report_array_from_automated_json_table' function
-- into the @report_data variable inside the compile-mysql.sh script

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_insert_helper_manual;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_insert_helper_manual(
    IN report_data JSON
)
BEGIN

    DECLARE report_count INT DEFAULT 0;
    DECLARE report_array_len INT;
    DECLARE report_enc_type_id INT DEFAULT NULL;
    DECLARE report_enc_type_uuid VARCHAR(50);
    DECLARE report_enc_name VARCHAR(500);

    SET session group_concat_max_len = 200000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO report_array_len;

    WHILE report_count < report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', report_count, ']')) INTO @report_data_item;
            SELECT JSON_EXTRACT(@report_data_item, '$.report_name') INTO report_enc_name;
            SELECT JSON_EXTRACT(@report_data_item, '$.encounter_type_uuid') INTO report_enc_type_uuid;

            SET report_enc_type_uuid = JSON_UNQUOTE(report_enc_type_uuid);

            SET report_enc_type_id = (SELECT DISTINCT et.encounter_type_id
                                      FROM icare_dim_encounter_type et
                                      WHERE et.uuid = report_enc_type_uuid
                                      LIMIT 1);

            IF report_enc_type_id IS NOT NULL THEN
                INSERT INTO icare_flat_table_config
                (report_name,
                 encounter_type_id,
                 table_json_data,
                 encounter_type_uuid)
                VALUES (JSON_UNQUOTE(report_enc_name),
                        report_enc_type_id,
                        @report_data_item,
                        report_enc_type_uuid);
            END IF;

            SET report_count = report_count + 1;

        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_insert_helper_auto  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_insert_helper_auto;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_insert_helper_auto()
main_block:
BEGIN

    DECLARE encounter_type_name CHAR(50) CHARACTER SET UTF8MB4;
    DECLARE is_automatic_flattening TINYINT(1);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounter_type_name CURSOR FOR
        SELECT DISTINCT et.name
        FROM icare_source_db.obs o
                 INNER JOIN icare_source_db.encounter e ON e.encounter_id = o.encounter_id
                 INNER JOIN icare_dim_encounter_type et ON e.encounter_type = et.encounter_type_id
        WHERE et.encounter_type_id NOT IN (SELECT DISTINCT tc.encounter_type_id from icare_flat_table_config tc)
          AND et.retired = 0;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT DISTINCT(automatic_flattening_mode_switch)
    INTO is_automatic_flattening
    FROM _icare_etl_user_settings;

    -- If auto-flattening is not switched on, do nothing
    IF is_automatic_flattening = 0 THEN
        LEAVE main_block;
    END IF;

    OPEN cursor_encounter_type_name;
    computations_loop:
    LOOP
        FETCH cursor_encounter_type_name INTO encounter_type_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO icare_flat_table_config(report_name, encounter_type_id, table_json_data, encounter_type_uuid)
                    SELECT
                        name,
                        encounter_type_id,
                         CONCAT(''{'',
                            ''"report_name": "'', name, ''", '',
                            ''"flat_table_name": "'', table_name, ''", '',
                            ''"encounter_type_uuid": "'', uuid, ''", '',
                            ''"table_columns": '', json_obj, '' '',
                            ''}'') AS table_json_data,
                        encounter_type_uuid
                    FROM (
                        SELECT DISTINCT
                            et.name,
                            encounter_type_id,
                            et.auto_flat_table_name AS table_name,
                            et.uuid, ',
                '(
                SELECT DISTINCT CONCAT(''{'', GROUP_CONCAT(CONCAT(''"'', name, ''":"'', uuid, ''"'') SEPARATOR '','' ),''}'') x
                FROM (
                        SELECT
                            DISTINCT et.encounter_type_id,
                            c.auto_table_column_name AS name,
                            c.uuid
                        FROM icare_source_db.obs o
                        INNER JOIN icare_source_db.encounter e
                                  ON e.encounter_id = o.encounter_id
                        INNER JOIN icare_dim_encounter_type et
                                  ON e.encounter_type = et.encounter_type_id
                        INNER JOIN icare_dim_concept c
                                  ON o.concept_id = c.concept_id
                        WHERE et.name = ''', encounter_type_name, '''
                                    AND et.retired = 0
                                ) json_obj
                        ) json_obj,
                       et.uuid as encounter_type_uuid
                    FROM icare_dim_encounter_type et
                    INNER JOIN icare_source_db.encounter e
                        ON e.encounter_type = et.encounter_type_id
                    WHERE et.name = ''', encounter_type_name, '''
                ) X  ;   ');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END LOOP computations_loop;
    CLOSE cursor_encounter_type_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_insert;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @report_data = '{"flat_report_metadata":[]}';

CALL sp_icare_flat_table_config_insert_helper_manual(@report_data); -- insert manually added config JSON data from config dir
CALL sp_icare_flat_table_config_insert_helper_auto(); -- insert automatically generated config JSON data from db

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_update;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the hash of the JSON data
UPDATE icare_flat_table_config
SET table_json_data_hash = MD5(TRIM(table_json_data))
WHERE id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_flat_table_config_create();
CALL sp_icare_flat_table_config_insert();
CALL sp_icare_flat_table_config_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental_create;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_incremental_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_incremental_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE IF NOT EXISTS icare_flat_table_config_incremental
(
    id                   INT           NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    encounter_type_id    INT           NOT NULL UNIQUE,
    report_name          VARCHAR(100)  NOT NULL,
    table_json_data      JSON          NOT NULL,
    table_json_data_hash CHAR(32)      NULL,
    encounter_type_uuid  CHAR(38)      NOT NULL,
    incremental_record   INT DEFAULT 0 NOT NULL COMMENT 'Whether `table_json_data` has been modified or not',

    INDEX icare_idx_encounter_type_id (encounter_type_id),
    INDEX icare_idx_report_name (report_name),
    INDEX icare_idx_table_json_data_hash (table_json_data_hash),
    INDEX icare_idx_uuid (encounter_type_uuid),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental_insert_helper_manual  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- manually extracts user given flat table config file json into the icare_flat_table_config_incremental table
-- this data together with automatically extracted flat table data is inserted into the icare_flat_table_config_incremental table
-- later it is processed by the 'fn_icare_generate_report_array_from_automated_json_table' function
-- into the @report_data variable inside the compile-mysql.sh script

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental_insert_helper_manual;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental_insert_helper_manual(
    IN report_data MEDIUMTEXT CHARACTER SET UTF8MB4
)
BEGIN

    DECLARE report_count INT DEFAULT 0;
    DECLARE report_array_len INT;
    DECLARE report_enc_type_id INT DEFAULT NULL;
    DECLARE report_enc_type_uuid VARCHAR(50);
    DECLARE report_enc_name VARCHAR(500);

    SET session group_concat_max_len = 20000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO report_array_len;

    WHILE report_count < report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', report_count, ']')) INTO @report_data_item;
            SELECT JSON_EXTRACT(@report_data_item, '$.report_name') INTO report_enc_name;
            SELECT JSON_EXTRACT(@report_data_item, '$.encounter_type_uuid') INTO report_enc_type_uuid;

            SET report_enc_type_uuid = JSON_UNQUOTE(report_enc_type_uuid);

            SET report_enc_type_id = (SELECT DISTINCT et.encounter_type_id
                                      FROM icare_dim_encounter_type et
                                      WHERE et.uuid = report_enc_type_uuid
                                      LIMIT 1);

            IF report_enc_type_id IS NOT NULL THEN
                INSERT INTO icare_flat_table_config_incremental
                (report_name,
                 encounter_type_id,
                 table_json_data,
                 encounter_type_uuid)
                VALUES (JSON_UNQUOTE(report_enc_name),
                        report_enc_type_id,
                        @report_data_item,
                        report_enc_type_uuid);
            END IF;

            SET report_count = report_count + 1;

        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental_insert_helper_auto  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental_insert_helper_auto;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental_insert_helper_auto()
main_block:
BEGIN

    DECLARE encounter_type_name CHAR(50) CHARACTER SET UTF8MB4;
    DECLARE is_automatic_flattening TINYINT(1);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounter_type_name CURSOR FOR
        SELECT DISTINCT et.name
        FROM icare_source_db.obs o
                 INNER JOIN icare_source_db.encounter e ON e.encounter_id = o.encounter_id
                 INNER JOIN icare_dim_encounter_type et ON e.encounter_type = et.encounter_type_id
        WHERE et.encounter_type_id NOT IN (SELECT DISTINCT tc.encounter_type_id from icare_flat_table_config_incremental tc)
          AND et.retired = 0;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT DISTINCT(automatic_flattening_mode_switch)
    INTO is_automatic_flattening
    FROM _icare_etl_user_settings;

    -- If auto-flattening is not switched on, do nothing
    IF is_automatic_flattening = 0 THEN
        LEAVE main_block;
    END IF;

    OPEN cursor_encounter_type_name;
    computations_loop:
    LOOP
        FETCH cursor_encounter_type_name INTO encounter_type_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO icare_flat_table_config_incremental (report_name, encounter_type_id, table_json_data, encounter_type_uuid)
                    SELECT
                        name,
                        encounter_type_id,
                         CONCAT(''{'',
                            ''"report_name": "'', name, ''", '',
                            ''"flat_table_name": "'', table_name, ''", '',
                            ''"encounter_type_uuid": "'', uuid, ''", '',
                            ''"table_columns": '', json_obj, '' '',
                            ''}'') AS table_json_data,
                        encounter_type_uuid
                    FROM (
                        SELECT DISTINCT
                            et.name,
                            encounter_type_id,
                            et.auto_flat_table_name AS table_name,
                            et.uuid, ',
                '(
                SELECT DISTINCT CONCAT(''{'', GROUP_CONCAT(CONCAT(''"'', name, ''":"'', uuid, ''"'') SEPARATOR '','' ),''}'') x
                FROM (
                        SELECT
                            DISTINCT et.encounter_type_id,
                            c.auto_table_column_name AS name,
                            c.uuid
                        FROM icare_source_db.obs o
                        INNER JOIN icare_source_db.encounter e
                                  ON e.encounter_id = o.encounter_id
                        INNER JOIN icare_dim_encounter_type et
                                  ON e.encounter_type = et.encounter_type_id
                        INNER JOIN icare_dim_concept c
                                  ON o.concept_id = c.concept_id
                        WHERE et.name = ''', encounter_type_name, '''
                                    AND et.retired = 0
                                ) json_obj
                        ) json_obj,
                       et.uuid as encounter_type_uuid
                    FROM icare_dim_encounter_type et
                    INNER JOIN icare_source_db.encounter e
                        ON e.encounter_type = et.encounter_type_id
                    WHERE et.name = ''', encounter_type_name, '''
                ) X  ;   ');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END LOOP computations_loop;
    CLOSE cursor_encounter_type_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @report_data = '{"flat_report_metadata":[]}';

CALL sp_icare_flat_table_config_incremental_insert_helper_manual(@report_data); -- insert manually added config JSON data from config dir
CALL sp_icare_flat_table_config_incremental_insert_helper_auto(); -- insert automatically generated config JSON data from db

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the hash of the JSON data
UPDATE icare_flat_table_config_incremental
SET table_json_data_hash = MD5(TRIM(table_json_data))
WHERE id > 0;

-- If a new encounter type has been added
INSERT INTO icare_flat_table_config (report_name,
                                     encounter_type_id,
                                     table_json_data,
                                     encounter_type_uuid,
                                     table_json_data_hash,
                                     incremental_record)
SELECT tci.report_name,
       tci.encounter_type_id,
       tci.table_json_data,
       tci.encounter_type_uuid,
       tci.table_json_data_hash,
       1
FROM icare_flat_table_config_incremental tci
WHERE tci.encounter_type_id NOT IN (SELECT encounter_type_id FROM icare_flat_table_config);

-- If there is any change in either concepts or encounter types in terms of names or additional questions
UPDATE icare_flat_table_config tc
    INNER JOIN icare_flat_table_config_incremental tci ON tc.encounter_type_id = tci.encounter_type_id
SET tc.table_json_data      = tci.table_json_data,
    tc.table_json_data_hash = tci.table_json_data_hash,
    tc.report_name          = tci.report_name,
    tc.encounter_type_uuid  = tci.encounter_type_uuid,
    tc.incremental_record   = 1
WHERE tc.table_json_data_hash <> tci.table_json_data_hash
  AND tc.table_json_data_hash IS NOT NULL;

-- If an encounter type has been voided then delete it from dim_json
DELETE
FROM icare_flat_table_config
WHERE encounter_type_id NOT IN (SELECT tci.encounter_type_id FROM icare_flat_table_config_incremental tci);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental_truncate;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_incremental_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_incremental_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_icare_truncate_table('icare_flat_table_config_incremental');
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_flat_table_config_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_flat_table_config_incremental;


~-~-
CREATE PROCEDURE sp_icare_flat_table_config_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_flat_table_config_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_flat_table_config_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_flat_table_config_incremental_create();
CALL sp_icare_flat_table_config_incremental_truncate();
CALL sp_icare_flat_table_config_incremental_insert();
CALL sp_icare_flat_table_config_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_obs_group  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_obs_group;


~-~-
CREATE PROCEDURE sp_icare_obs_group()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_obs_group', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_obs_group', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_obs_group_create();
CALL sp_icare_obs_group_insert();
CALL sp_icare_obs_group_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_obs_group_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_obs_group_create;


~-~-
CREATE PROCEDURE sp_icare_obs_group_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_obs_group_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_obs_group_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_obs_group
(
    id                     INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    obs_id                 INT          NOT NULL,
    obs_group_concept_id   INT          NOT NULL,
    obs_group_concept_name VARCHAR(255) NOT NULL, -- should be the concept name of the obs
    obs_group_id           INT          NOT NULL,

    INDEX icare_idx_obs_id (obs_id),
    INDEX icare_idx_obs_group_concept_id (obs_group_concept_id),
    INDEX icare_idx_obs_group_concept_name (obs_group_concept_name)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_obs_group_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_obs_group_insert;


~-~-
CREATE PROCEDURE sp_icare_obs_group_insert()
BEGIN
    DECLARE total_records INT;
    DECLARE batch_size INT DEFAULT 1000000; -- 1 million records per batch
    DECLARE icare_offset INT DEFAULT 0;

    -- Calculate total records to process
SELECT COUNT(*)
INTO total_records
FROM icare_source_db.obs o
         INNER JOIN icare_dim_encounter e ON o.encounter_id = e.encounter_id
         INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                     FROM icare_concept_metadata) md ON o.concept_id = md.concept_id
WHERE o.encounter_id IS NOT NULL;

-- Loop through the batches of records
WHILE icare_offset < total_records
    DO
        -- Create a temporary table to store obs group information
        CREATE TEMPORARY TABLE icare_temp_obs_group_ids
        (
            obs_group_id INT NOT NULL,
            row_num      INT NOT NULL,
            INDEX icare_idx_obs_group_id (obs_group_id),
            INDEX icare_idx_row_num (row_num)
        )
        CHARSET = UTF8MB4;

        -- Insert into the temporary table based on obs group aggregation
        SET @sql_temp_insert = CONCAT('
            INSERT INTO icare_temp_obs_group_ids
            SELECT obs_group_id, COUNT(*) AS row_num
            FROM icare_z_encounter_obs o
            WHERE obs_group_id IS NOT NULL
            GROUP BY obs_group_id, person_id, encounter_id
            LIMIT ', batch_size, ' OFFSET ', icare_offset);

PREPARE stmt_temp_insert FROM @sql_temp_insert;
EXECUTE stmt_temp_insert;
DEALLOCATE PREPARE stmt_temp_insert;

-- Insert into the final table from the temp table, including concept data
SET @sql_obs_group_insert = CONCAT('
            INSERT INTO icare_obs_group (obs_group_concept_id, obs_group_concept_name, obs_id,obs_group_id)
            SELECT DISTINCT o.obs_question_concept_id,
                            LEFT(c.auto_table_column_name, 12) AS name,
                            o.obs_id,
                            o.obs_group_id
            FROM icare_temp_obs_group_ids t
                     INNER JOIN icare_z_encounter_obs o ON t.obs_group_id = o.obs_group_id
                     INNER JOIN icare_dim_concept c ON o.obs_question_concept_id = c.concept_id
            WHERE t.row_num > 1
            LIMIT ', batch_size, ' OFFSET ', icare_offset);

PREPARE stmt_obs_group_insert FROM @sql_obs_group_insert;
EXECUTE stmt_obs_group_insert;
DEALLOCATE PREPARE stmt_obs_group_insert;

-- Drop the temporary table after processing each batch
DROP TEMPORARY TABLE IF EXISTS icare_temp_obs_group_ids;

        -- Increment the offset for the next batch
        SET icare_offset = icare_offset + batch_size;

END WHILE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_obs_group_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_obs_group_update;


~-~-
CREATE PROCEDURE sp_icare_obs_group_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_obs_group_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_obs_group_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_error_log_drop  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_error_log_drop;


~-~-
CREATE PROCEDURE sp_icare_etl_error_log_drop()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_error_log_drop', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_error_log_drop', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

DROP TABLE IF EXISTS _icare_etl_error_log;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_error_log_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_error_log_create;


~-~-
CREATE PROCEDURE sp_icare_etl_error_log_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_error_log_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_error_log_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE _icare_etl_error_log
(
    id             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key',
    procedure_name VARCHAR(255) NOT NULL,
    error_message  VARCHAR(1000),
    error_code     INT,
    sql_state      VARCHAR(5),
    error_time     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_error_log_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_error_log_insert;


~-~-
CREATE PROCEDURE sp_icare_etl_error_log_insert(
    IN procedure_name VARCHAR(255),
    IN error_message VARCHAR(1000),
    IN error_code INT,
    IN sql_state VARCHAR(5)
)
BEGIN
    INSERT INTO _icare_etl_error_log (procedure_name, error_message, error_code, sql_state)
    VALUES (procedure_name, error_message, error_code, sql_state);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_error_log  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_error_log;


~-~-
CREATE PROCEDURE sp_icare_etl_error_log()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_error_log', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_error_log', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_error_log_drop();
CALL sp_icare_etl_error_log_create();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_user_settings_drop  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_user_settings_drop;


~-~-
CREATE PROCEDURE sp_icare_etl_user_settings_drop()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_user_settings_drop', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_user_settings_drop', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

DROP TABLE IF EXISTS _icare_etl_user_settings;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_user_settings_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_user_settings_create;


~-~-
CREATE PROCEDURE sp_icare_etl_user_settings_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_user_settings_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_user_settings_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE _icare_etl_user_settings
(
    id                               INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'Primary Key',
    openmrs_database                 VARCHAR(255) NOT NULL COMMENT 'Name of the OpenMRS (source) database',
    etl_database                     VARCHAR(255) NOT NULL COMMENT 'Name of the ETL (target) database',
    concepts_locale                  CHAR(4)      NOT NULL COMMENT 'Preferred Locale of the Concept names',
    table_partition_number           INT          NOT NULL COMMENT 'Number of columns at which to partition \'many columned\' Tables',
    incremental_mode_switch          TINYINT(1)   NOT NULL COMMENT 'If MambaETL should/not run in Incremental Mode',
    automatic_flattening_mode_switch TINYINT(1)   NOT NULL COMMENT 'If MambaETL should/not automatically flatten ALL encounter types',
    etl_interval_seconds             INT          NOT NULL COMMENT 'ETL Runs every 60 seconds',
    incremental_mode_switch_cascaded TINYINT(1)   NOT NULL DEFAULT 0 COMMENT 'This is a computed Incremental Mode (1 or 0) for the ETL that is cascaded down to the implementer scripts',
    last_etl_schedule_insert_id      INT          NOT NULL DEFAULT 1 COMMENT 'Insert ID of the last ETL that ran'

) CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_user_settings_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_user_settings_insert;


~-~-
CREATE PROCEDURE sp_icare_etl_user_settings_insert(
    IN openmrs_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN etl_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    SET @insert_stmt = CONCAT(
            'INSERT INTO _icare_etl_user_settings (`openmrs_database`, `etl_database`, `concepts_locale`, `table_partition_number`, `incremental_mode_switch`, `automatic_flattening_mode_switch`, `etl_interval_seconds`) VALUES (''',
            openmrs_database, ''', ''', etl_database, ''', ''', concepts_locale, ''', ', table_partition_number, ', ', incremental_mode_switch, ', ', automatic_flattening_mode_switch, ', ', etl_interval_seconds, ');');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_user_settings  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_user_settings;


~-~-
CREATE PROCEDURE sp_icare_etl_user_settings(
    IN openmrs_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN etl_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    -- DECLARE openmrs_db VARCHAR(256)  DEFAULT IFNULL(openmrs_database, 'openmrs');

    CALL sp_icare_etl_user_settings_drop();
    CALL sp_icare_etl_user_settings_create();
    CALL sp_icare_etl_user_settings_insert(openmrs_database,
                                           etl_database,
                                           concepts_locale,
                                           table_partition_number,
                                           incremental_mode_switch,
                                           automatic_flattening_mode_switch,
                                           etl_interval_seconds);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_all_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_all_create;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_all_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_incremental_columns_index_all_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_incremental_columns_index_all_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This table will be used to index the columns that are used to determine if a record is new, changed, retired or voided
-- It will be used to speed up the incremental updates for each incremental Table indentified in the ETL process

CREATE TABLE IF NOT EXISTS icare_etl_incremental_columns_index_all
(
    incremental_table_pkey INT        NOT NULL UNIQUE PRIMARY KEY,

    date_created           DATETIME   NOT NULL,
    date_changed           DATETIME   NULL,
    date_retired           DATETIME   NULL,
    date_voided            DATETIME   NULL,

    retired                TINYINT(1) NULL,
    voided                 TINYINT(1) NULL,

    INDEX icare_idx_date_created (date_created),
    INDEX icare_idx_date_changed (date_changed),
    INDEX icare_idx_date_retired (date_retired),
    INDEX icare_idx_date_voided (date_voided),
    INDEX icare_idx_retired (retired),
    INDEX icare_idx_voided (voided)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_all_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_all_insert;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_all_insert(
    IN openmrs_table VARCHAR(255)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE incremental_column_name VARCHAR(255);
    DECLARE column_list VARCHAR(500) DEFAULT 'incremental_table_pkey, ';
    DECLARE select_list VARCHAR(500) DEFAULT '';
    DECLARE pkey_column VARCHAR(255);

    DECLARE column_cursor CURSOR FOR
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'icare_etl_incremental_columns_index_all';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Identify the primary key of the target table
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'icare_source_db'
      AND TABLE_NAME = openmrs_table
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    -- Add the primary key to the select list
    SET select_list = CONCAT(select_list, pkey_column, ', ');

    OPEN column_cursor;

    column_loop:
    LOOP
        FETCH column_cursor INTO incremental_column_name;
        IF done THEN
            LEAVE column_loop;
        END IF;

        -- Check if the column exists in openmrs_table
        IF EXISTS (SELECT 1
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'icare_source_db'
                     AND TABLE_NAME = openmrs_table
                     AND COLUMN_NAME = incremental_column_name) THEN
            SET column_list = CONCAT(column_list, incremental_column_name, ', ');
            SET select_list = CONCAT(select_list, incremental_column_name, ', ');
        END IF;
    END LOOP column_loop;

    CLOSE column_cursor;

    -- Remove the trailing comma and space
    SET column_list = LEFT(column_list, CHAR_LENGTH(column_list) - 2);
    SET select_list = LEFT(select_list, CHAR_LENGTH(select_list) - 2);

    SET @insert_sql = CONCAT(
            'INSERT INTO icare_etl_incremental_columns_index_all (', column_list, ') ',
            'SELECT ', select_list, ' FROM icare_source_db.', openmrs_table
                      );

    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_all_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_all_truncate;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_all_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_incremental_columns_index_all_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_incremental_columns_index_all_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE icare_etl_incremental_columns_index_all;
-- CALL sp_icare_truncate_table('icare_etl_incremental_columns_index_all');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_all;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_all(
    IN target_table_name VARCHAR(255)
)
BEGIN

    CALL sp_icare_etl_incremental_columns_index_all_create();
    CALL sp_icare_etl_incremental_columns_index_all_truncate();
    CALL sp_icare_etl_incremental_columns_index_all_insert(target_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_new_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_new_create;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_new_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_incremental_columns_index_new_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_incremental_columns_index_new_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This Table will only contain Primary keys for only those records that are NEW (i.e. Newly Inserted)

CREATE TEMPORARY TABLE IF NOT EXISTS icare_etl_incremental_columns_index_new
(
    incremental_table_pkey INT NOT NULL UNIQUE PRIMARY KEY
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_new_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_new_insert;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_new_insert(
    IN icare_table_name VARCHAR(255)
)
BEGIN
    DECLARE incremental_start_time DATETIME;
    DECLARE pkey_column VARCHAR(255);

    -- Identify the primary key of the 'icare_table_name'
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = icare_table_name
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET incremental_start_time = (SELECT start_time
                                  FROM _icare_etl_schedule sch
                                  WHERE end_time IS NOT NULL
                                    AND transaction_status = 'COMPLETED'
                                  ORDER BY id DESC
                                  LIMIT 1);

    -- Insert only records that are NOT in the icare ETL table
    -- and were created after the last ETL run time (start_time)
    SET @insert_sql = CONCAT(
            'INSERT INTO icare_etl_incremental_columns_index_new (incremental_table_pkey) ',
            'SELECT DISTINCT incremental_table_pkey ',
            'FROM icare_etl_incremental_columns_index_all ',
            'WHERE date_created >= ?',
            ' AND incremental_table_pkey NOT IN (SELECT DISTINCT (', pkey_column, ') FROM ', icare_table_name, ')');

    PREPARE stmt FROM @insert_sql;
    SET @inc_start_time = incremental_start_time;
    EXECUTE stmt USING @inc_start_time;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_new_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_new_truncate;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_new_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_incremental_columns_index_new_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_incremental_columns_index_new_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE icare_etl_incremental_columns_index_new;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_new  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_new;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_new(
    IN icare_table_name VARCHAR(255)
)
BEGIN

    CALL sp_icare_etl_incremental_columns_index_new_create();
    CALL sp_icare_etl_incremental_columns_index_new_truncate();
    CALL sp_icare_etl_incremental_columns_index_new_insert(icare_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_modified_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_modified_create;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_modified_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_incremental_columns_index_modified_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_incremental_columns_index_modified_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This Table will only contain Primary keys for only those records that have been modified/updated (i.e. Retired, Voided, Changed)

CREATE TEMPORARY TABLE IF NOT EXISTS icare_etl_incremental_columns_index_modified
(
    incremental_table_pkey INT NOT NULL UNIQUE PRIMARY KEY
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_modified_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_modified_insert;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_modified_insert(
    IN icare_table_name VARCHAR(255)
)
BEGIN
    DECLARE incremental_start_time DATETIME;
    DECLARE pkey_column VARCHAR(255);

    -- Identify the primary key of the 'icare_table_name'
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = icare_table_name
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET incremental_start_time = (SELECT start_time
                                  FROM _icare_etl_schedule sch
                                  WHERE end_time IS NOT NULL
                                    AND transaction_status = 'COMPLETED'
                                  ORDER BY id DESC
                                  LIMIT 1);

    -- Insert only records that are NOT in the icare ETL table
    -- and were created after the last ETL run time (start_time)
    SET @insert_sql = CONCAT(
            'INSERT INTO icare_etl_incremental_columns_index_modified (incremental_table_pkey) ',
            'SELECT DISTINCT incremental_table_pkey ',
            'FROM icare_etl_incremental_columns_index_all ',
            'WHERE date_changed >= ?',
            ' OR (voided = 1 AND date_voided >= ?)',
            ' OR (retired = 1 AND date_retired >= ?)');

    PREPARE stmt FROM @insert_sql;
    SET @incremental_start_time = incremental_start_time;
    EXECUTE stmt USING @incremental_start_time, @incremental_start_time, @incremental_start_time;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_modified_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_modified_truncate;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_modified_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_etl_incremental_columns_index_modified_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_etl_incremental_columns_index_modified_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE icare_etl_incremental_columns_index_modified;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index_modified  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index_modified;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index_modified(
    IN icare_table_name VARCHAR(255)
)
BEGIN

    CALL sp_icare_etl_incremental_columns_index_modified_create();
    CALL sp_icare_etl_incremental_columns_index_modified_truncate();
    CALL sp_icare_etl_incremental_columns_index_modified_insert(icare_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_etl_incremental_columns_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_etl_incremental_columns_index;


~-~-
CREATE PROCEDURE sp_icare_etl_incremental_columns_index(
    IN openmrs_table_name VARCHAR(255),
    IN icare_table_name VARCHAR(255)
)
BEGIN

    CALL sp_icare_etl_incremental_columns_index_all(openmrs_table_name);
    CALL sp_icare_etl_incremental_columns_index_new(icare_table_name);
    CALL sp_icare_etl_incremental_columns_index_modified(icare_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_table_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_table_insert(
    IN openmrs_table VARCHAR(255),
    IN icare_table VARCHAR(255),
    IN is_incremental BOOLEAN
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE tbl_column_name VARCHAR(255);
    DECLARE column_list VARCHAR(500) DEFAULT '';
    DECLARE select_list VARCHAR(500) DEFAULT '';
    DECLARE pkey_column VARCHAR(255);
    DECLARE join_clause VARCHAR(500) DEFAULT '';

    DECLARE column_cursor CURSOR FOR
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = icare_table;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Identify the primary key of the icare_source_db table
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'icare_source_db'
      AND TABLE_NAME = openmrs_table
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET column_list = CONCAT(column_list, 'incremental_record', ', ');
    IF is_incremental THEN
        SET select_list = CONCAT(select_list, 1, ', ');
    ELSE
        SET select_list = CONCAT(select_list, 0, ', ');
    END IF;

    OPEN column_cursor;

    column_loop:
    LOOP
        FETCH column_cursor INTO tbl_column_name;
        IF done THEN
            LEAVE column_loop;
        END IF;

        -- Check if the column exists in openmrs_table
        IF EXISTS (SELECT 1
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'icare_source_db'
                     AND TABLE_NAME = openmrs_table
                     AND COLUMN_NAME = tbl_column_name) THEN
            SET column_list = CONCAT(column_list, tbl_column_name, ', ');
            SET select_list = CONCAT(select_list, tbl_column_name, ', ');
        END IF;
    END LOOP column_loop;

    CLOSE column_cursor;

    -- Remove the trailing comma and space
    SET column_list = LEFT(column_list, CHAR_LENGTH(column_list) - 2);
    SET select_list = LEFT(select_list, CHAR_LENGTH(select_list) - 2);

    -- Set the join clause if it is an incremental insert
    IF is_incremental THEN
        SET join_clause = CONCAT(
                ' INNER JOIN icare_etl_incremental_columns_index_new ic',
                ' ON tb.', pkey_column, ' = ic.incremental_table_pkey');
    END IF;

    SET @insert_sql = CONCAT(
            'INSERT INTO ', icare_table, ' (', column_list, ') ',
            'SELECT ', select_list,
            ' FROM icare_source_db.', openmrs_table, ' tb',
            join_clause, ';');

    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_truncate_table  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_truncate_table;


~-~-
CREATE PROCEDURE sp_icare_truncate_table(
    IN table_to_truncate VARCHAR(64) CHARACTER SET UTF8MB4
)
BEGIN
    IF EXISTS (SELECT 1
               FROM information_schema.tables
               WHERE table_schema = DATABASE()
                 AND table_name = table_to_truncate) THEN

        SET @sql = CONCAT('TRUNCATE TABLE ', table_to_truncate);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_drop_table  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_drop_table;


~-~-
CREATE PROCEDURE sp_icare_drop_table(
    IN table_to_drop VARCHAR(64) CHARACTER SET UTF8MB4
)
BEGIN

    SET @sql = CONCAT('DROP TABLE IF EXISTS ', table_to_drop);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location_create;


~-~-
CREATE PROCEDURE sp_icare_dim_location_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_location
(
    location_id        INT           NOT NULL UNIQUE PRIMARY KEY,
    name               VARCHAR(255)  NOT NULL,
    description        VARCHAR(255)  NULL,
    city_village       VARCHAR(255)  NULL,
    state_province     VARCHAR(255)  NULL,
    postal_code        VARCHAR(50)   NULL,
    country            VARCHAR(50)   NULL,
    latitude           VARCHAR(50)   NULL,
    longitude          VARCHAR(50)   NULL,
    county_district    VARCHAR(255)  NULL,
    address1           VARCHAR(255)  NULL,
    address2           VARCHAR(255)  NULL,
    address3           VARCHAR(255)  NULL,
    address4           VARCHAR(255)  NULL,
    address5           VARCHAR(255)  NULL,
    address6           VARCHAR(255)  NULL,
    address7           VARCHAR(255)  NULL,
    address8           VARCHAR(255)  NULL,
    address9           VARCHAR(255)  NULL,
    address10          VARCHAR(255)  NULL,
    address11          VARCHAR(255)  NULL,
    address12          VARCHAR(255)  NULL,
    address13          VARCHAR(255)  NULL,
    address14          VARCHAR(255)  NULL,
    address15          VARCHAR(255)  NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_retired       DATETIME      NULL,
    retired            TINYINT(1)    NULL,
    retire_reason      VARCHAR(255)  NULL,
    retired_by         INT           NULL,
    changed_by         INT           NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_name (name),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_location_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('location', 'icare_dim_location', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location_update;


~-~-
CREATE PROCEDURE sp_icare_dim_location_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location;


~-~-
CREATE PROCEDURE sp_icare_dim_location()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_location_create();
CALL sp_icare_dim_location_insert();
CALL sp_icare_dim_location_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_location_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('location', 'icare_dim_location');
CALL sp_icare_dim_location_incremental_insert();
CALL sp_icare_dim_location_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_location_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('location', 'icare_dim_location', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_location_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_location_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_location_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_location_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_location_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_location mdl
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mdl.location_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.location l
    ON mdl.location_id = l.location_id
SET mdl.name               = l.name,
    mdl.description        = l.description,
    mdl.city_village       = l.city_village,
    mdl.state_province     = l.state_province,
    mdl.postal_code        = l.postal_code,
    mdl.country            = l.country,
    mdl.latitude           = l.latitude,
    mdl.longitude          = l.longitude,
    mdl.county_district    = l.county_district,
    mdl.address1           = l.address1,
    mdl.address2           = l.address2,
    mdl.address3           = l.address3,
    mdl.address4           = l.address4,
    mdl.address5           = l.address5,
    mdl.address6           = l.address6,
    mdl.address7           = l.address7,
    mdl.address8           = l.address8,
    mdl.address9           = l.address9,
    mdl.address10          = l.address10,
    mdl.address11          = l.address11,
    mdl.address12          = l.address12,
    mdl.address13          = l.address13,
    mdl.address14          = l.address14,
    mdl.address15          = l.address15,
    mdl.date_created       = l.date_created,
    mdl.changed_by         = l.changed_by,
    mdl.date_changed       = l.date_changed,
    mdl.retired            = l.retired,
    mdl.retired_by         = l.retired_by,
    mdl.date_retired       = l.date_retired,
    mdl.retire_reason      = l.retire_reason,
    mdl.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type_create;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_patient_identifier_type
(
    patient_identifier_type_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                       VARCHAR(50)   NOT NULL,
    description                TEXT          NULL,
    uuid                       CHAR(38)      NOT NULL,
    date_created               DATETIME      NOT NULL,
    date_changed               DATETIME      NULL,
    date_retired               DATETIME      NULL,
    retired                    TINYINT(1)    NULL,
    retire_reason              VARCHAR(255)  NULL,
    retired_by                 INT           NULL,
    changed_by                 INT           NULL,
    incremental_record         INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_name (name),
    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('patient_identifier_type', 'icare_dim_patient_identifier_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type_update;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_patient_identifier_type_create();
CALL sp_icare_dim_patient_identifier_type_insert();
CALL sp_icare_dim_patient_identifier_type_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('patient_identifier_type', 'icare_dim_patient_identifier_type');
CALL sp_icare_dim_patient_identifier_type_incremental_insert();
CALL sp_icare_dim_patient_identifier_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('patient_identifier_type', 'icare_dim_patient_identifier_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_type_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_patient_identifier_type mdpit
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mdpit.patient_identifier_type_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.patient_identifier_type pit
    ON mdpit.patient_identifier_type_id = pit.patient_identifier_type_id
SET mdpit.name               = pit.name,
    mdpit.description        = pit.description,
    mdpit.uuid               = pit.uuid,
    mdpit.date_created       = pit.date_created,
    mdpit.date_changed       = pit.date_changed,
    mdpit.date_retired       = pit.date_retired,
    mdpit.retired            = pit.retired,
    mdpit.retire_reason      = pit.retire_reason,
    mdpit.retired_by         = pit.retired_by,
    mdpit.changed_by         = pit.changed_by,
    mdpit.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_datatype_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_datatype_create;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_datatype_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_datatype_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_datatype_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_concept_datatype
(
    concept_datatype_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                VARCHAR(255)  NOT NULL,
    hl7_abbreviation    VARCHAR(3)    NULL,
    description         VARCHAR(255)  NULL,
    date_created        DATETIME      NOT NULL,
    date_retired        DATETIME      NULL,
    retired             TINYINT(1)    NULL,
    retire_reason       VARCHAR(255)  NULL,
    retired_by          INT           NULL,
    incremental_record  INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_name (name),
    INDEX icare_idx_retired (retired),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_datatype_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_datatype_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_datatype_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_datatype_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_datatype_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('concept_datatype', 'icare_dim_concept_datatype', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_datatype  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_datatype;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_datatype()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_datatype', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_datatype', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_concept_datatype_create();
CALL sp_icare_dim_concept_datatype_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_datatype_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_datatype_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_datatype_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_datatype_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_datatype_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('concept_datatype', 'icare_dim_concept_datatype', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_datatype_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_datatype_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_datatype_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_datatype_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_datatype_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_concept_datatype mcd
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mcd.concept_datatype_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.concept_datatype cd
    ON mcd.concept_datatype_id = cd.concept_datatype_id
SET mcd.name               = cd.name,
    mcd.hl7_abbreviation   = cd.hl7_abbreviation,
    mcd.description        = cd.description,
    mcd.date_created       = cd.date_created,
    mcd.date_retired       = cd.date_retired,
    mcd.retired            = cd.retired,
    mcd.retired_by         = cd.retired_by,
    mcd.retire_reason      = cd.retire_reason,
    mcd.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_datatype_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_datatype_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_datatype_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_datatype_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_datatype_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('concept_datatype', 'icare_dim_concept_datatype');
CALL sp_icare_dim_concept_datatype_incremental_insert();
CALL sp_icare_dim_concept_datatype_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_create;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_concept
(
    concept_id             INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                   CHAR(38)      NOT NULL,
    datatype_id            INT           NOT NULL, -- make it a FK
    datatype               VARCHAR(100)  NULL,
    name                   VARCHAR(256)  NULL,
    auto_table_column_name VARCHAR(60)   NULL,
    date_created           DATETIME      NOT NULL,
    date_changed           DATETIME      NULL,
    date_retired           DATETIME      NULL,
    retired                TINYINT(1)    NULL,
    retire_reason          VARCHAR(255)  NULL,
    retired_by             INT           NULL,
    changed_by             INT           NULL,
    incremental_record     INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_datatype_id (datatype_id),
    INDEX icare_idx_retired (retired),
    INDEX icare_idx_date_created (date_created),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('concept', 'icare_dim_concept', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_update;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Data Type
UPDATE icare_dim_concept c
    INNER JOIN icare_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.name
WHERE c.concept_id > 0;

CREATE TEMPORARY TABLE icare_temp_computed_concept_name
(
    concept_id      INT          NOT NULL,
    computed_name   VARCHAR(255) NOT NULL,
    tbl_column_name VARCHAR(60)  NOT NULL,
    INDEX icare_idx_concept_id (concept_id)
)
    CHARSET = UTF8MB4
SELECT c.concept_id,
       CASE
           WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = '' THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
           WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
           ELSE TRIM(cn.name)
           END     AS computed_name,
       TRIM(LOWER(
               LEFT(
                       REPLACE(
                               REPLACE(
                                       fn_icare_remove_special_characters(
                                           -- First collapse multiple spaces into one
                                               fn_icare_collapse_spaces(
                                                       TRIM(
                                                               CASE
                                                                   WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = ''
                                                                       THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
                                                                   WHEN c.retired = 1
                                                                       THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
                                                                   ELSE TRIM(cn.name)
                                                                   END
                                                       )
                                               )
                                       ),
                                       ' ', '_'), -- Replace single spaces with underscores
                               '__', '_'), -- Replace double underscores with a single underscore
                       60 -- Limit to 60 characters
               ))) AS tbl_column_name
FROM icare_dim_concept c
         LEFT JOIN icare_dim_concept_name cn ON c.concept_id = cn.concept_id;

UPDATE icare_dim_concept c
    INNER JOIN icare_temp_computed_concept_name tc
    ON c.concept_id = tc.concept_id
SET c.name                   = tc.computed_name,
    c.auto_table_column_name = IF(tc.tbl_column_name = '',
                                  CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id),
                                  tc.tbl_column_name)
WHERE c.concept_id > 0;

DROP TEMPORARY TABLE IF EXISTS icare_temp_computed_concept_name;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_cleanup;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_table_column_name VARCHAR(60);
    DECLARE previous_auto_table_column_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT concept_id, auto_table_column_name
        FROM icare_dim_concept
        ORDER BY auto_table_column_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS icare_dim_concept_temp
    (
        concept_id             INT,
        auto_table_column_name VARCHAR(60)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE icare_dim_concept_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_table_column_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_table_column_name IS NULL THEN
            SET current_auto_table_column_name = '';
        END IF;

        IF current_auto_table_column_name = previous_auto_table_column_name THEN

            SET counter = counter + 1;
            SET current_auto_table_column_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_table_column_name) <= 57,
                       previous_auto_table_column_name,
                       LEFT(previous_auto_table_column_name, CHAR_LENGTH(previous_auto_table_column_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_table_column_name = current_auto_table_column_name;
        END IF;

        INSERT INTO icare_dim_concept_temp (concept_id, auto_table_column_name)
        VALUES (current_id, current_auto_table_column_name);

    END LOOP;

    CLOSE cur;

    UPDATE icare_dim_concept c
        JOIN icare_dim_concept_temp t
        ON c.concept_id = t.concept_id
    SET c.auto_table_column_name = t.auto_table_column_name
    WHERE c.concept_id > 0;

    DROP TEMPORARY TABLE IF EXISTS icare_dim_concept_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept;


~-~-
CREATE PROCEDURE sp_icare_dim_concept()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_concept_create();
CALL sp_icare_dim_concept_insert();
CALL sp_icare_dim_concept_update();
CALL sp_icare_dim_concept_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('concept', 'icare_dim_concept', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_concept tc
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON tc.concept_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.concept sc
    ON tc.concept_id = sc.concept_id
SET tc.uuid               = sc.uuid,
    tc.datatype_id        = sc.datatype_id,
    tc.date_created       = sc.date_created,
    tc.date_changed       = sc.date_changed,
    tc.date_retired       = sc.date_retired,
    tc.changed_by         = sc.changed_by,
    tc.retired            = sc.retired,
    tc.retired_by         = sc.retired_by,
    tc.retire_reason      = sc.retire_reason,
    tc.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- Update the Data Type
UPDATE icare_dim_concept c
    INNER JOIN icare_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.name
WHERE c.incremental_record = 1;

-- Update the concept name and table column name
CREATE TEMPORARY TABLE icare_temp_computed_concept_name
(
    concept_id      INT          NOT NULL,
    computed_name   VARCHAR(255) NOT NULL,
    tbl_column_name VARCHAR(60)  NOT NULL,
    INDEX icare_idx_concept_id (concept_id)
)CHARSET = UTF8MB4 AS
SELECT c.concept_id,
       CASE
           WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = '' THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
           WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
           ELSE TRIM(cn.name)
           END                                                         AS computed_name,
       TRIM(LOWER(LEFT(REPLACE(REPLACE(fn_icare_remove_special_characters(
                                               CASE
                                                   WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = ''
                                                       THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
                                                   WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
                                                   ELSE TRIM(cn.name)
                                                   END
                                       ), ' ', '_'), '__', '_'), 60))) AS tbl_column_name
FROM icare_dim_concept c
         LEFT JOIN icare_dim_concept_name cn ON c.concept_id = cn.concept_id;

UPDATE icare_dim_concept c
    INNER JOIN icare_temp_computed_concept_name tc
    ON c.concept_id = tc.concept_id
SET c.name                   = tc.computed_name,
    c.auto_table_column_name = IF(tc.tbl_column_name = '',
                                  CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id),
                                  tc.tbl_column_name)
WHERE c.incremental_record = 1;

DROP TEMPORARY TABLE IF EXISTS icare_temp_computed_concept_name;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_incremental_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_table_column_name VARCHAR(60);
    DECLARE previous_auto_table_column_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT concept_id, auto_table_column_name
        FROM icare_dim_concept
        WHERE incremental_record = 1
        ORDER BY auto_table_column_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS icare_dim_concept_temp
    (
        concept_id             INT,
        auto_table_column_name VARCHAR(60)

    ) CHARSET = UTF8MB4;

    TRUNCATE TABLE icare_dim_concept_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_table_column_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_table_column_name IS NULL THEN
            SET current_auto_table_column_name = '';
        END IF;

        IF current_auto_table_column_name = previous_auto_table_column_name THEN

            SET counter = counter + 1;
            SET current_auto_table_column_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_table_column_name) <= 57,
                       previous_auto_table_column_name,
                       LEFT(previous_auto_table_column_name, CHAR_LENGTH(previous_auto_table_column_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_table_column_name = current_auto_table_column_name;
        END IF;

        INSERT INTO icare_dim_concept_temp (concept_id, auto_table_column_name)
        VALUES (current_id, current_auto_table_column_name);

    END LOOP;

    CLOSE cur;

    UPDATE icare_dim_concept c
        JOIN icare_dim_concept_temp t
        ON c.concept_id = t.concept_id
    SET c.auto_table_column_name = t.auto_table_column_name
    WHERE incremental_record = 1;

    DROP TEMPORARY TABLE icare_dim_concept_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('concept', 'icare_dim_concept');
CALL sp_icare_dim_concept_incremental_insert();
CALL sp_icare_dim_concept_incremental_update();
CALL sp_icare_dim_concept_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_answer_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_answer_create;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_answer_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_answer_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_answer_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_concept_answer
(
    concept_answer_id  INT           NOT NULL UNIQUE PRIMARY KEY,
    concept_id         INT           NOT NULL,
    answer_concept     INT,
    answer_drug        INT,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_concept_answer (concept_answer_id),
    INDEX icare_idx_concept_id (concept_id),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_answer_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_answer_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_answer_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_answer_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_answer_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('concept_answer', 'icare_dim_concept_answer', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_answer  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_answer;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_answer()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_answer', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_answer', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_concept_answer_create();
CALL sp_icare_dim_concept_answer_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_answer_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_answer_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_answer_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_answer_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_answer_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new records
CALL sp_icare_dim_table_insert('concept_answer', 'icare_dim_concept_answer', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_answer_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_answer_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_answer_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_answer_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_answer_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_answer_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_answer_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_answer_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_answer_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_answer_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('concept_answer', 'icare_dim_concept_answer');
CALL sp_icare_dim_concept_answer_incremental_insert();
CALL sp_icare_dim_concept_answer_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_create;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_concept_name
(
    concept_name_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    concept_id         INT,
    name               VARCHAR(255)  NOT NULL,
    locale             VARCHAR(50)   NOT NULL,
    locale_preferred   TINYINT,
    concept_name_type  VARCHAR(255),
    voided             TINYINT,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_concept_id (concept_id),
    INDEX icare_idx_concept_name_type (concept_name_type),
    INDEX icare_idx_locale (locale),
    INDEX icare_idx_locale_preferred (locale_preferred),
    INDEX icare_idx_voided (voided),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

INSERT INTO icare_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    voided,
                                    concept_name_type,
                                    date_created,
                                    date_changed,
                                    changed_by,
                                    voided_by,
                                    date_voided,
                                    void_reason)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.voided,
       cn.concept_name_type,
       cn.date_created,
       cn.date_changed,
       cn.changed_by,
       cn.voided_by,
       cn.date_voided,
       cn.void_reason
FROM icare_source_db.concept_name cn
WHERE cn.locale COLLATE utf8mb4_general_ci IN (SELECT DISTINCT(concepts_locale) COLLATE utf8mb4_general_ci FROM _icare_etl_user_settings)
  AND IF(cn.locale_preferred = 1, cn.locale_preferred = 1, cn.concept_name_type COLLATE utf8mb4_general_ci = 'FULLY_SPECIFIED' COLLATE utf8mb4_general_ci)
  AND cn.voided = 0;
-- Use locale preferred or Fully specified name

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_update;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_concept_name_create();
CALL sp_icare_dim_concept_name_insert();
CALL sp_icare_dim_concept_name_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
INSERT INTO icare_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    voided,
                                    concept_name_type,
                                    date_created,
                                    date_changed,
                                    changed_by,
                                    voided_by,
                                    date_voided,
                                    void_reason,
                                    incremental_record)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.voided,
       cn.concept_name_type,
       cn.date_created,
       cn.date_changed,
       cn.changed_by,
       cn.voided_by,
       cn.date_voided,
       cn.void_reason,
       1
FROM icare_source_db.concept_name cn
         INNER JOIN icare_etl_incremental_columns_index_new ic
                    ON cn.concept_name_id = ic.incremental_table_pkey
WHERE cn.locale IN (SELECT DISTINCT (concepts_locale) FROM _icare_etl_user_settings)
  AND cn.locale_preferred = 1
  AND cn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- Update only Modified Records
UPDATE icare_dim_concept_name cn
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON cn.concept_name_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.concept_name cnm
    ON cn.concept_name_id = cnm.concept_name_id
SET cn.concept_id         = cnm.concept_id,
    cn.name               = cnm.name,
    cn.locale             = cnm.locale,
    cn.locale_preferred   = cnm.locale_preferred,
    cn.concept_name_type  = cnm.concept_name_type,
    cn.voided             = cnm.voided,
    cn.date_created       = cnm.date_created,
    cn.date_changed       = cnm.date_changed,
    cn.changed_by         = cnm.changed_by,
    cn.voided_by          = cnm.voided_by,
    cn.date_voided        = cnm.date_voided,
    cn.void_reason        = cnm.void_reason,
    cn.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_incremental_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_incremental_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_incremental_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- Delete any concept names that have become voided or not locale_preferred or not our locale we set (so we are consistent with the original INSERT statement)
-- We only need to keep the non-voided, locale we set & locale_preferred concept names
-- This is because when concept names are modified, the old name is voided and a new name is created but both have a date_changed value of the same date (donno why)

DELETE
FROM icare_dim_concept_name
WHERE voided <> 0
   OR locale_preferred <> 1
   OR locale NOT IN (SELECT DISTINCT(concepts_locale) FROM _icare_etl_user_settings);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_concept_name_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_concept_name_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_concept_name_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_concept_name_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_concept_name_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('concept_name', 'icare_dim_concept_name');
CALL sp_icare_dim_concept_name_incremental_insert();
CALL sp_icare_dim_concept_name_incremental_update();
CALL sp_icare_dim_concept_name_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_create;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_encounter_type
(
    encounter_type_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                 CHAR(38)      NOT NULL,
    name                 VARCHAR(50)   NOT NULL,
    auto_flat_table_name VARCHAR(60)   NULL,
    description          TEXT          NULL,
    retired              TINYINT(1)    NULL,
    date_created         DATETIME      NULL,
    date_changed         DATETIME      NULL,
    changed_by           INT           NULL,
    date_retired         DATETIME      NULL,
    retired_by           INT           NULL,
    retire_reason        VARCHAR(255)  NULL,
    incremental_record   INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_retired (retired),
    INDEX icare_idx_name (name),
    INDEX icare_idx_auto_flat_table_name (auto_flat_table_name),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('encounter_type', 'icare_dim_encounter_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_update;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

UPDATE icare_dim_encounter_type et
SET et.auto_flat_table_name = LOWER(LEFT(
        REPLACE(REPLACE(fn_icare_remove_special_characters(CONCAT('icare_flat_encounter_', et.name)), ' ', '_'), '__',
                '_'), 60))
WHERE et.encounter_type_id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_cleanup;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_flat_table_name VARCHAR(60);
    DECLARE previous_auto_flat_table_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT encounter_type_id, auto_flat_table_name
        FROM icare_dim_encounter_type
        ORDER BY auto_flat_table_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS icare_dim_encounter_type_temp
    (
        encounter_type_id    INT,
        auto_flat_table_name VARCHAR(60)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE icare_dim_encounter_type_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_flat_table_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_flat_table_name IS NULL THEN
            SET current_auto_flat_table_name = '';
        END IF;

        IF current_auto_flat_table_name = previous_auto_flat_table_name THEN

            SET counter = counter + 1;
            SET current_auto_flat_table_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_flat_table_name) <= 57,
                       previous_auto_flat_table_name,
                       LEFT(previous_auto_flat_table_name, CHAR_LENGTH(previous_auto_flat_table_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_flat_table_name = current_auto_flat_table_name;
        END IF;

        INSERT INTO icare_dim_encounter_type_temp (encounter_type_id, auto_flat_table_name)
        VALUES (current_id, current_auto_flat_table_name);

    END LOOP;

    CLOSE cur;

    UPDATE icare_dim_encounter_type c
        JOIN icare_dim_encounter_type_temp t
        ON c.encounter_type_id = t.encounter_type_id
    SET c.auto_flat_table_name = t.auto_flat_table_name
    WHERE c.encounter_type_id > 0;

    DROP TEMPORARY TABLE icare_dim_encounter_type_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_encounter_type_create();
CALL sp_icare_dim_encounter_type_insert();
CALL sp_icare_dim_encounter_type_update();
CALL sp_icare_dim_encounter_type_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('encounter_type', 'icare_dim_encounter_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounter types
UPDATE icare_dim_encounter_type et
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON et.encounter_type_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.encounter_type ent
    ON et.encounter_type_id = ent.encounter_type_id
SET et.uuid               = ent.uuid,
    et.name               = ent.name,
    et.description        = ent.description,
    et.retired            = ent.retired,
    et.date_created       = ent.date_created,
    et.date_changed       = ent.date_changed,
    et.changed_by         = ent.changed_by,
    et.date_retired       = ent.date_retired,
    et.retired_by         = ent.retired_by,
    et.retire_reason      = ent.retire_reason,
    et.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

UPDATE icare_dim_encounter_type et
SET et.auto_flat_table_name = LOWER(LEFT(
        REPLACE(REPLACE(fn_icare_remove_special_characters(CONCAT('icare_flat_encounter_', et.name)), ' ', '_'), '__',
                '_'), 60))
WHERE et.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_incremental_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_flat_table_name VARCHAR(60);
    DECLARE previous_auto_flat_table_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT encounter_type_id, auto_flat_table_name
        FROM icare_dim_encounter_type
        WHERE incremental_record = 1
        ORDER BY auto_flat_table_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS icare_dim_encounter_type_temp
    (
        encounter_type_id    INT,
        auto_flat_table_name VARCHAR(60)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE icare_dim_encounter_type_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_flat_table_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_flat_table_name IS NULL THEN
            SET current_auto_flat_table_name = '';
        END IF;

        IF current_auto_flat_table_name = previous_auto_flat_table_name THEN

            SET counter = counter + 1;
            SET current_auto_flat_table_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_flat_table_name) <= 57,
                       previous_auto_flat_table_name,
                       LEFT(previous_auto_flat_table_name, CHAR_LENGTH(previous_auto_flat_table_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_flat_table_name = current_auto_flat_table_name;
        END IF;

        INSERT INTO icare_dim_encounter_type_temp (encounter_type_id, auto_flat_table_name)
        VALUES (current_id, current_auto_flat_table_name);

    END LOOP;

    CLOSE cur;

    UPDATE icare_dim_encounter_type et
        JOIN icare_dim_encounter_type_temp t
        ON et.encounter_type_id = t.encounter_type_id
    SET et.auto_flat_table_name = t.auto_flat_table_name
    WHERE et.incremental_record = 1;

    DROP TEMPORARY TABLE icare_dim_encounter_type_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_type_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('encounter_type', 'icare_dim_encounter_type');
CALL sp_icare_dim_encounter_type_incremental_insert();
CALL sp_icare_dim_encounter_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_create;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_encounter
(
    encounter_id        INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                CHAR(38)      NOT NULL,
    encounter_type      INT           NOT NULL,
    encounter_type_uuid CHAR(38)      NULL,
    patient_id          INT           NOT NULL,
    visit_id            INT           NULL,
    encounter_datetime  DATETIME      NOT NULL,
    date_created        DATETIME      NOT NULL,
    date_changed        DATETIME      NULL,
    changed_by          INT           NULL,
    date_voided         DATETIME      NULL,
    voided              TINYINT(1)    NOT NULL,
    voided_by           INT           NULL,
    void_reason         VARCHAR(255)  NULL,
    incremental_record  INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_encounter_id (encounter_id),
    INDEX icare_idx_encounter_type (encounter_type),
    INDEX icare_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX icare_idx_patient_id (patient_id),
    INDEX icare_idx_visit_id (visit_id),
    INDEX icare_idx_encounter_datetime (encounter_datetime),
    INDEX icare_idx_voided (voided),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

INSERT INTO icare_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 visit_id,
                                 encounter_datetime,
                                 date_created,
                                 date_changed,
                                 changed_by,
                                 date_voided,
                                 voided,
                                 voided_by,
                                 void_reason)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime,
       e.date_created,
       e.date_changed,
       e.changed_by,
       e.date_voided,
       e.voided,
       e.voided_by,
       e.void_reason
FROM icare_source_db.encounter e
         INNER JOIN icare_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id
WHERE et.uuid
          IN (SELECT DISTINCT(md.encounter_type_uuid)
              FROM icare_concept_metadata md);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_update;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_encounter_create();
CALL sp_icare_dim_encounter_insert();
CALL sp_icare_dim_encounter_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new records
INSERT INTO icare_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 visit_id,
                                 encounter_datetime,
                                 date_created,
                                 date_changed,
                                 changed_by,
                                 date_voided,
                                 voided,
                                 voided_by,
                                 void_reason,
                                 incremental_record)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime,
       e.date_created,
       e.date_changed,
       e.changed_by,
       e.date_voided,
       e.voided,
       e.voided_by,
       e.void_reason,
       1
FROM icare_source_db.encounter e
         INNER JOIN icare_etl_incremental_columns_index_new ic
                    ON e.encounter_id = ic.incremental_table_pkey
         INNER JOIN icare_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE icare_dim_encounter e
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON e.encounter_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.encounter enc
    ON e.encounter_id = enc.encounter_id
    INNER JOIN icare_dim_encounter_type et
    ON e.encounter_type = et.encounter_type_id
SET e.encounter_id        = enc.encounter_id,
    e.uuid                = enc.uuid,
    e.encounter_type      = enc.encounter_type,
    e.encounter_type_uuid = et.uuid,
    e.patient_id          = enc.patient_id,
    e.visit_id            = enc.visit_id,
    e.encounter_datetime  = enc.encounter_datetime,
    e.date_created        = enc.date_created,
    e.date_changed        = enc.date_changed,
    e.changed_by          = enc.changed_by,
    e.date_voided         = enc.date_voided,
    e.voided              = enc.voided,
    e.voided_by           = enc.voided_by,
    e.void_reason         = enc.void_reason,
    e.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_encounter_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_encounter_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_encounter_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_encounter_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_encounter_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('encounter', 'icare_dim_encounter');
CALL sp_icare_dim_encounter_incremental_insert();
CALL sp_icare_dim_encounter_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_report_definition_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_report_definition_create;


~-~-
CREATE PROCEDURE sp_icare_dim_report_definition_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_report_definition_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_report_definition_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_report_definition
(
    id                            INT          NOT NULL AUTO_INCREMENT,
    report_id                     VARCHAR(255) NOT NULL UNIQUE,
    report_procedure_name         VARCHAR(255) NOT NULL UNIQUE, -- should be derived from report_id??
    report_columns_procedure_name VARCHAR(255) NOT NULL UNIQUE,
    report_size_procedure_name    VARCHAR(255) NULL UNIQUE,
    sql_query                     TEXT         NOT NULL,
    table_name                    VARCHAR(255) NOT NULL,        -- name of the table (will contain columns) of this query
    report_name                   VARCHAR(255) NULL,
    result_column_names           TEXT         NULL,            -- comma-separated column names

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX icare_dim_report_definition_report_id_index
    ON icare_dim_report_definition (report_id);


CREATE TABLE icare_dim_report_definition_parameters
(
    id                 INT          NOT NULL AUTO_INCREMENT,
    report_id          VARCHAR(255) NOT NULL,
    parameter_name     VARCHAR(255) NOT NULL,
    parameter_type     VARCHAR(30)  NOT NULL,
    parameter_position INT          NOT NULL, -- takes order or declaration in JSON file

    PRIMARY KEY (id),
    FOREIGN KEY (`report_id`) REFERENCES `icare_dim_report_definition` (`report_id`)
)
    CHARSET = UTF8MB4;

CREATE INDEX icare_dim_report_definition_parameter_position_index
    ON icare_dim_report_definition_parameters (parameter_position);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_report_definition_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_report_definition_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_report_definition_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_report_definition_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_report_definition_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
SET @report_definition_json = '{
  "report_definitions": []
}';
CALL sp_icare_extract_report_definition_metadata(@report_definition_json, 'icare_dim_report_definition');
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_report_definition_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_report_definition_update;


~-~-
CREATE PROCEDURE sp_icare_dim_report_definition_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_report_definition_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_report_definition_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_report_definition  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_report_definition;


~-~-
CREATE PROCEDURE sp_icare_dim_report_definition()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_report_definition', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_report_definition', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_report_definition_create();
CALL sp_icare_dim_report_definition_insert();
CALL sp_icare_dim_report_definition_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_create;


~-~-
CREATE PROCEDURE sp_icare_dim_person_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_person
(
    person_id           INT           NOT NULL UNIQUE PRIMARY KEY,
    birthdate           DATE          NULL,
    birthdate_estimated TINYINT(1)    NOT NULL,
    age                 INT           NULL,
    dead                TINYINT(1)    NOT NULL,
    death_date          DATETIME      NULL,
    deathdate_estimated TINYINT       NOT NULL,
    gender              VARCHAR(50)   NULL,
    person_name_short   VARCHAR(255)  NULL,
    person_name_long    TEXT          NULL,
    uuid                CHAR(38)      NOT NULL,
    date_created        DATETIME      NOT NULL,
    date_changed        DATETIME      NULL,
    changed_by          INT           NULL,
    date_voided         DATETIME      NULL,
    voided              TINYINT(1)    NOT NULL,
    voided_by           INT           NULL,
    void_reason         VARCHAR(255)  NULL,
    incremental_record  INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_person_id (person_id),
    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_incremental_record (incremental_record)

) CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person', 'icare_dim_person', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

UPDATE icare_dim_person psn
    INNER JOIN icare_dim_person_name pn
    on psn.person_id = pn.person_id
SET age               = fn_icare_age_calculator(psn.birthdate, psn.death_date),
    person_name_short = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name),
    person_name_long  = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name_prefix, pn.family_name,
                                  pn.family_name2,
                                  pn.family_name_suffix, pn.degree)
WHERE pn.preferred = 1
  AND pn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person;


~-~-
CREATE PROCEDURE sp_icare_dim_person()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_person_create();
CALL sp_icare_dim_person_insert();
CALL sp_icare_dim_person_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person', 'icare_dim_person', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Persons
UPDATE icare_dim_person p
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON p.person_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.person psn
    ON p.person_id = psn.person_id
SET p.birthdate           = psn.birthdate,
    p.birthdate_estimated = psn.birthdate_estimated,
    p.dead                = psn.dead,
    p.death_date          = psn.death_date,
    p.deathdate_estimated = psn.deathdate_estimated,
    p.gender              = psn.gender,
    p.uuid                = psn.uuid,
    p.date_created        = psn.date_created,
    p.date_changed        = psn.date_changed,
    p.changed_by          = psn.changed_by,
    p.date_voided         = psn.date_voided,
    p.voided              = psn.voided,
    p.voided_by           = psn.voided_by,
    p.void_reason         = psn.void_reason,
    p.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

UPDATE icare_dim_person psn
    INNER JOIN icare_dim_person_name pn
    on psn.person_id = pn.person_id
SET age               = fn_icare_age_calculator(psn.birthdate, psn.death_date),
    person_name_short = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name),
    person_name_long  = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name_prefix, pn.family_name,
                                  pn.family_name2,
                                  pn.family_name_suffix, pn.degree)
WHERE psn.incremental_record = 1
  AND pn.preferred = 1
  AND pn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_person_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('person', 'icare_dim_person');
CALL sp_icare_dim_person_incremental_insert();
CALL sp_icare_dim_person_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_create;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_person_attribute
(
    person_attribute_id      INT           NOT NULL UNIQUE PRIMARY KEY,
    person_attribute_type_id INT           NOT NULL,
    person_id                INT           NOT NULL,
    uuid                     CHAR(38)      NOT NULL,
    value                    NVARCHAR(50)  NOT NULL,
    voided                   TINYINT,
    date_created             DATETIME      NOT NULL,
    date_changed             DATETIME      NULL,
    date_voided              DATETIME      NULL,
    changed_by               INT           NULL,
    voided_by                INT           NULL,
    void_reason              VARCHAR(255)  NULL,
    incremental_record       INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_person_attribute_type_id (person_attribute_type_id),
    INDEX icare_idx_person_id (person_id),
    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_voided (voided),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person_attribute', 'icare_dim_person_attribute', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_person_attribute_create();
CALL sp_icare_dim_person_attribute_insert();
CALL sp_icare_dim_person_attribute_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person_attribute', 'icare_dim_person_attribute', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Persons
UPDATE icare_dim_person_attribute mpa
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mpa.person_attribute_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.person_attribute pa
    ON mpa.person_attribute_id = pa.person_attribute_id
SET mpa.person_attribute_id = pa.person_attribute_id,
    mpa.person_id           = pa.person_id,
    mpa.uuid                = pa.uuid,
    mpa.value               = pa.value,
    mpa.date_created        = pa.date_created,
    mpa.date_changed        = pa.date_changed,
    mpa.date_voided         = pa.date_voided,
    mpa.changed_by          = pa.changed_by,
    mpa.voided              = pa.voided,
    mpa.voided_by           = pa.voided_by,
    mpa.void_reason         = pa.void_reason,
    mpa.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('person_attribute', 'icare_dim_person_attribute');
CALL sp_icare_dim_person_attribute_incremental_insert();
CALL sp_icare_dim_person_attribute_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type_create;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_person_attribute_type
(
    person_attribute_type_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                     NVARCHAR(50)  NOT NULL,
    description              TEXT          NULL,
    searchable               TINYINT(1)    NOT NULL,
    uuid                     NVARCHAR(50)  NOT NULL,
    date_created             DATETIME      NOT NULL,
    date_changed             DATETIME      NULL,
    date_retired             DATETIME      NULL,
    retired                  TINYINT(1)    NULL,
    retire_reason            VARCHAR(255)  NULL,
    retired_by               INT           NULL,
    changed_by               INT           NULL,
    incremental_record       INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_name (name),
    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_retired (retired),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person_attribute_type', 'icare_dim_person_attribute_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_person_attribute_type_create();
CALL sp_icare_dim_person_attribute_type_insert();
CALL sp_icare_dim_person_attribute_type_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('person_attribute_type', 'icare_dim_person_attribute_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_person_attribute_type mpat
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mpat.person_attribute_type_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.person_attribute_type pat
    ON mpat.person_attribute_type_id = pat.person_attribute_type_id
SET mpat.name               = pat.name,
    mpat.description        = pat.description,
    mpat.searchable         = pat.searchable,
    mpat.uuid               = pat.uuid,
    mpat.date_created       = pat.date_created,
    mpat.date_changed       = pat.date_changed,
    mpat.date_retired       = pat.date_retired,
    mpat.changed_by         = pat.changed_by,
    mpat.retired            = pat.retired,
    mpat.retired_by         = pat.retired_by,
    mpat.retire_reason      = pat.retire_reason,
    mpat.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_attribute_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_attribute_type_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_person_attribute_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_attribute_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_attribute_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('person_attribute_type', 'icare_dim_person_attribute_type');
CALL sp_icare_dim_person_attribute_type_incremental_insert();
CALL sp_icare_dim_person_attribute_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_create;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_patient_identifier
(
    patient_identifier_id INT           NOT NULL UNIQUE PRIMARY KEY,
    patient_id            INT           NOT NULL,
    identifier            VARCHAR(50)   NOT NULL,
    identifier_type       INT           NOT NULL,
    preferred             TINYINT       NOT NULL,
    location_id           INT           NULL,
    patient_program_id    INT           NULL,
    uuid                  CHAR(38)      NOT NULL,
    date_created          DATETIME      NOT NULL,
    date_changed          DATETIME      NULL,
    date_voided           DATETIME      NULL,
    changed_by            INT           NULL,
    voided                TINYINT,
    voided_by             INT           NULL,
    void_reason           VARCHAR(255)  NULL,
    incremental_record    INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_patient_id (patient_id),
    INDEX icare_idx_identifier (identifier),
    INDEX icare_idx_identifier_type (identifier_type),
    INDEX icare_idx_preferred (preferred),
    INDEX icare_idx_voided (voided),
    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('patient_identifier', 'icare_dim_patient_identifier', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_update;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_patient_identifier_create();
CALL sp_icare_dim_patient_identifier_insert();
CALL sp_icare_dim_patient_identifier_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('patient_identifier', 'icare_dim_patient_identifier', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_patient_identifier mpi
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mpi.patient_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.patient_identifier pi
    ON mpi.patient_id = pi.patient_id
SET mpi.patient_id         = pi.patient_id,
    mpi.identifier         = pi.identifier,
    mpi.identifier_type    = pi.identifier_type,
    mpi.preferred          = pi.preferred,
    mpi.location_id        = pi.location_id,
    mpi.patient_program_id = pi.patient_program_id,
    mpi.uuid               = pi.uuid,
    mpi.voided             = pi.voided,
    mpi.date_created       = pi.date_created,
    mpi.date_changed       = pi.date_changed,
    mpi.date_voided        = pi.date_voided,
    mpi.changed_by         = pi.changed_by,
    mpi.voided_by          = pi.voided_by,
    mpi.void_reason        = pi.void_reason,
    mpi.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_patient_identifier_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_patient_identifier_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_patient_identifier_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_patient_identifier_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_patient_identifier_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('patient_identifier', 'icare_dim_patient_identifier');
CALL sp_icare_dim_patient_identifier_incremental_insert();
CALL sp_icare_dim_patient_identifier_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_name_create;


~-~-
CREATE PROCEDURE sp_icare_dim_person_name_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_name_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_name_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_person_name
(
    person_name_id     INT           NOT NULL UNIQUE PRIMARY KEY,
    person_id          INT           NOT NULL,
    preferred          TINYINT       NOT NULL,
    prefix             VARCHAR(50)   NULL,
    given_name         VARCHAR(50)   NULL,
    middle_name        VARCHAR(50)   NULL,
    family_name_prefix VARCHAR(50)   NULL,
    family_name        VARCHAR(50)   NULL,
    family_name2       VARCHAR(50)   NULL,
    family_name_suffix VARCHAR(50)   NULL,
    degree             VARCHAR(50)   NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided             TINYINT(1)    NOT NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_person_id (person_id),
    INDEX icare_idx_voided (voided),
    INDEX icare_idx_preferred (preferred),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_name_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_name_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_name_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_name_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person_name', 'icare_dim_person_name', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_name  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_name;


~-~-
CREATE PROCEDURE sp_icare_dim_person_name()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_name', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_name', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_person_name_create();
CALL sp_icare_dim_person_name_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_name_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_name_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_name_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_name_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_name_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person_name', 'icare_dim_person_name', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_name_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_name_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_name_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_name_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_name_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE icare_dim_person_name dpn
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON dpn.person_name_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.person_name pn
    ON dpn.person_name_id = pn.person_name_id
SET dpn.person_name_id     = pn.person_name_id,
    dpn.person_id          = pn.person_id,
    dpn.preferred          = pn.preferred,
    dpn.prefix             = pn.prefix,
    dpn.given_name         = pn.given_name,
    dpn.middle_name        = pn.middle_name,
    dpn.family_name_prefix = pn.family_name_prefix,
    dpn.family_name        = pn.family_name,
    dpn.family_name2       = pn.family_name2,
    dpn.family_name_suffix = pn.family_name_suffix,
    dpn.degree             = pn.degree,
    dpn.date_created       = pn.date_created,
    dpn.date_changed       = pn.date_changed,
    dpn.changed_by         = pn.changed_by,
    dpn.date_voided        = pn.date_voided,
    dpn.voided             = pn.voided,
    dpn.voided_by          = pn.voided_by,
    dpn.void_reason        = pn.void_reason,
    dpn.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_name_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_name_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_person_name_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_name_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_name_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('person_name', 'icare_dim_person_name');
CALL sp_icare_dim_person_name_incremental_insert();
CALL sp_icare_dim_person_name_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_address_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_address_create;


~-~-
CREATE PROCEDURE sp_icare_dim_person_address_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_address_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_address_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_person_address
(
    person_address_id  INT           NOT NULL UNIQUE PRIMARY KEY,
    person_id          INT           NULL,
    preferred          TINYINT       NOT NULL,
    address1           VARCHAR(255)  NULL,
    address2           VARCHAR(255)  NULL,
    address3           VARCHAR(255)  NULL,
    address4           VARCHAR(255)  NULL,
    address5           VARCHAR(255)  NULL,
    address6           VARCHAR(255)  NULL,
    address7           VARCHAR(255)  NULL,
    address8           VARCHAR(255)  NULL,
    address9           VARCHAR(255)  NULL,
    address10          VARCHAR(255)  NULL,
    address11          VARCHAR(255)  NULL,
    address12          VARCHAR(255)  NULL,
    address13          VARCHAR(255)  NULL,
    address14          VARCHAR(255)  NULL,
    address15          VARCHAR(255)  NULL,
    city_village       VARCHAR(255)  NULL,
    county_district    VARCHAR(255)  NULL,
    state_province     VARCHAR(255)  NULL,
    postal_code        VARCHAR(50)   NULL,
    country            VARCHAR(50)   NULL,
    latitude           VARCHAR(50)   NULL,
    longitude          VARCHAR(50)   NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided             TINYINT,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_person_id (person_id),
    INDEX icare_idx_preferred (preferred),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_address_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_address_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_address_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_address_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_address_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('person_address', 'icare_dim_person_address', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_address  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_address;


~-~-
CREATE PROCEDURE sp_icare_dim_person_address()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_address', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_address', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_person_address_create();
CALL sp_icare_dim_person_address_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_address_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_address_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_person_address_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_address_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_address_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('person_address', 'icare_dim_person_address', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_address_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_address_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_person_address_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_address_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_address_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE icare_dim_person_address mpa
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON mpa.person_address_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.person_address pa
    ON mpa.person_address_id = pa.person_address_id
SET mpa.person_id          = pa.person_id,
    mpa.preferred          = pa.preferred,
    mpa.address1           = pa.address1,
    mpa.address2           = pa.address2,
    mpa.address3           = pa.address3,
    mpa.address4           = pa.address4,
    mpa.address5           = pa.address5,
    mpa.address6           = pa.address6,
    mpa.address7           = pa.address7,
    mpa.address8           = pa.address8,
    mpa.address9           = pa.address9,
    mpa.address10          = pa.address10,
    mpa.address11          = pa.address11,
    mpa.address12          = pa.address12,
    mpa.address13          = pa.address13,
    mpa.address14          = pa.address14,
    mpa.address15          = pa.address15,
    mpa.city_village       = pa.city_village,
    mpa.county_district    = pa.county_district,
    mpa.state_province     = pa.state_province,
    mpa.postal_code        = pa.postal_code,
    mpa.country            = pa.country,
    mpa.latitude           = pa.latitude,
    mpa.longitude          = pa.longitude,
    mpa.date_created       = pa.date_created,
    mpa.date_changed       = pa.date_changed,
    mpa.date_voided        = pa.date_voided,
    mpa.changed_by         = pa.changed_by,
    mpa.voided             = pa.voided,
    mpa.voided_by          = pa.voided_by,
    mpa.void_reason        = pa.void_reason,
    mpa.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_person_address_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_person_address_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_person_address_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_person_address_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_person_address_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('person_address', 'icare_dim_person_address');
CALL sp_icare_dim_person_address_incremental_insert();
CALL sp_icare_dim_person_address_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user_create;


~-~-
CREATE PROCEDURE sp_icare_dim_user_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE icare_dim_users
(
    user_id            INT           NOT NULL UNIQUE PRIMARY KEY,
    system_id          VARCHAR(50)   NOT NULL,
    username           VARCHAR(50)   NULL,
    creator            INT           NOT NULL,
    person_id          INT           NOT NULL,
    uuid               CHAR(38)      NOT NULL,
    email              VARCHAR(255)  NULL,
    retired            TINYINT(1)    NULL,
    date_created       DATETIME      NULL,
    date_changed       DATETIME      NULL,
    changed_by         INT           NULL,
    date_retired       DATETIME      NULL,
    retired_by         INT           NULL,
    retire_reason      VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_system_id (system_id),
    INDEX icare_idx_username (username),
    INDEX icare_idx_retired (retired),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_user_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('users', 'icare_dim_users', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user_update;


~-~-
CREATE PROCEDURE sp_icare_dim_user_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user;


~-~-
CREATE PROCEDURE sp_icare_dim_user()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
    CALL sp_icare_dim_user_create();
    CALL sp_icare_dim_user_insert();
    CALL sp_icare_dim_user_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_user_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_icare_dim_table_insert('users', 'icare_dim_users', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_user_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Users
UPDATE icare_dim_users u
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON u.user_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.users us
    ON u.user_id = us.user_id
SET u.system_id          = us.system_id,
    u.username           = us.username,
    u.creator            = us.creator,
    u.person_id          = us.person_id,
    u.uuid               = us.uuid,
    u.email              = us.email,
    u.retired            = us.retired,
    u.date_created       = us.date_created,
    u.date_changed       = us.date_changed,
    u.changed_by         = us.changed_by,
    u.date_retired       = us.date_retired,
    u.retired_by         = us.retired_by,
    u.retire_reason      = us.retire_reason,
    u.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_user_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_user_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_user_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_user_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_user_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_icare_etl_incremental_columns_index('users', 'icare_dim_users');
CALL sp_icare_dim_user_incremental_insert();
CALL sp_icare_dim_user_incremental_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship_create;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_relationship
(
    relationship_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    person_a           INT           NOT NULL,
    relationship       INT           NOT NULL,
    person_b           INT           NOT NULL,
    start_date         DATETIME      NULL,
    end_date           DATETIME      NULL,
    creator            INT           NOT NULL,
    uuid               CHAR(38)      NOT NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    changed_by         INT           NULL,
    date_voided        DATETIME      NULL,
    voided             TINYINT(1)    NOT NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_person_a (person_a),
    INDEX icare_idx_person_b (person_b),
    INDEX icare_idx_relationship (relationship),
    INDEX icare_idx_incremental_record (incremental_record)

) CHARSET = UTF8MB3;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('relationship', 'icare_dim_relationship', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship_update;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_relationship_create();
CALL sp_icare_dim_relationship_insert();
CALL sp_icare_dim_relationship_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('relationship', 'icare_dim_relationship', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only modified records
UPDATE icare_dim_relationship r
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON r.relationship_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.relationship rel
    ON r.relationship_id = rel.relationship_id
SET r.relationship       = rel.relationship,
    r.person_a           = rel.person_a,
    r.relationship       = rel.relationship,
    r.person_b           = rel.person_b,
    r.start_date         = rel.start_date,
    r.end_date           = rel.end_date,
    r.creator            = rel.creator,
    r.uuid               = rel.uuid,
    r.date_created       = rel.date_created,
    r.date_changed       = rel.date_changed,
    r.changed_by         = rel.changed_by,
    r.voided             = rel.voided,
    r.voided_by          = rel.voided_by,
    r.date_voided        = rel.date_voided,
    r.incremental_record = 1
WHERE im.incremental_table_pkey > 1;


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_relationship_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_relationship_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_relationship_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_relationship_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_relationship_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('relationship', 'icare_dim_relationship');
CALL sp_icare_dim_relationship_incremental_insert();
CALL sp_icare_dim_relationship_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders_create;


~-~-
CREATE PROCEDURE sp_icare_dim_orders_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_orders
(
    order_id               INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                   CHAR(38)      NOT NULL,
    order_type_id          INT           NOT NULL,
    concept_id             INT           NOT NULL,
    patient_id             INT           NOT NULL,
    encounter_id           INT           NOT NULL, -- links with encounter table
    accession_number       VARCHAR(255)  NULL,
    order_number           VARCHAR(50)   NOT NULL,
    orderer                INT           NOT NULL,
    instructions           TEXT          NULL,
    date_activated         DATETIME      NULL,
    auto_expire_date       DATETIME      NULL,
    date_stopped           DATETIME      NULL,
    order_reason           INT           NULL,
    order_reason_non_coded VARCHAR(255)  NULL,
    urgency                VARCHAR(50)   NOT NULL,
    previous_order_id      INT           NULL,
    order_action           VARCHAR(50)   NOT NULL,
    comment_to_fulfiller   VARCHAR(1024) NULL,
    care_setting           INT           NOT NULL,
    scheduled_date         DATETIME      NULL,
    order_group_id         INT           NULL,
    sort_weight            DOUBLE        NULL,
    fulfiller_comment      VARCHAR(1024) NULL,
    fulfiller_status       VARCHAR(50)   NULL,
    date_created           DATETIME      NOT NULL,
    creator                INT           NULL,
    voided                 TINYINT(1)    NOT NULL,
    voided_by              INT           NULL,
    date_voided            DATETIME      NULL,
    void_reason            VARCHAR(255)  NULL,
    incremental_record     INT DEFAULT 0 NOT NULL,

    INDEX icare_idx_uuid (uuid),
    INDEX icare_idx_order_type_id (order_type_id),
    INDEX icare_idx_concept_id (concept_id),
    INDEX icare_idx_patient_id (patient_id),
    INDEX icare_idx_encounter_id (encounter_id),
    INDEX icare_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_orders_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('orders', 'icare_dim_orders', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders_update;


~-~-
CREATE PROCEDURE sp_icare_dim_orders_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders;


~-~-
CREATE PROCEDURE sp_icare_dim_orders()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_orders_create();
CALL sp_icare_dim_orders_insert();
CALL sp_icare_dim_orders_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_orders_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_table_insert('orders', 'icare_dim_orders', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_dim_orders_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE icare_dim_orders do
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON do.order_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.orders o
    ON do.order_id = o.order_id
SET do.order_id               = o.order_id,
    do.uuid                   = o.uuid,
    do.order_type_id          = o.order_type_id,
    do.concept_id             = o.concept_id,
    do.patient_id             = o.patient_id,
    do.encounter_id           = o.encounter_id,
    do.accession_number       = o.accession_number,
    do.order_number           = o.order_number,
    do.orderer                = o.orderer,
    do.instructions           = o.instructions,
    do.date_activated         = o.date_activated,
    do.auto_expire_date       = o.auto_expire_date,
    do.date_stopped           = o.date_stopped,
    do.order_reason           = o.order_reason,
    do.order_reason_non_coded = o.order_reason_non_coded,
    do.urgency                = o.urgency,
    do.previous_order_id      = o.previous_order_id,
    do.order_action           = o.order_action,
    do.comment_to_fulfiller   = o.comment_to_fulfiller,
    do.care_setting           = o.care_setting,
    do.scheduled_date         = o.scheduled_date,
    do.order_group_id         = o.order_group_id,
    do.sort_weight            = o.sort_weight,
    do.fulfiller_comment      = o.fulfiller_comment,
    do.fulfiller_status       = o.fulfiller_status,
    do.date_created           = o.date_created,
    do.creator                = o.creator,
    do.voided                 = o.voided,
    do.voided_by              = o.voided_by,
    do.date_voided            = o.date_voided,
    do.void_reason            = o.void_reason,
    do.incremental_record     = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_orders_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_orders_incremental;


~-~-
CREATE PROCEDURE sp_icare_dim_orders_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_orders_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_orders_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('orders', 'icare_dim_orders');
CALL sp_icare_dim_orders_incremental_insert();
CALL sp_icare_dim_orders_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_agegroup_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_agegroup_create;


~-~-
CREATE PROCEDURE sp_icare_dim_agegroup_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_agegroup_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_agegroup_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_dim_agegroup
(
    id              INT         NOT NULL AUTO_INCREMENT,
    age             INT         NULL,
    datim_agegroup  VARCHAR(50) NULL,
    datim_age_val   INT         NULL,
    normal_agegroup VARCHAR(50) NULL,
    normal_age_val   INT        NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_agegroup_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_agegroup_insert;


~-~-
CREATE PROCEDURE sp_icare_dim_agegroup_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_agegroup_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_agegroup_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_icare_load_agegroup();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_agegroup_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_agegroup_update;


~-~-
CREATE PROCEDURE sp_icare_dim_agegroup_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_agegroup_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_agegroup_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- update age_value b
UPDATE icare_dim_agegroup a
SET datim_age_val =
    CASE
        WHEN a.datim_agegroup = '<1' THEN 1
        WHEN a.datim_agegroup = '1-4' THEN 2
        WHEN a.datim_agegroup = '5-9' THEN 3
        WHEN a.datim_agegroup = '10-14' THEN 4
        WHEN a.datim_agegroup = '15-19' THEN 5
        WHEN a.datim_agegroup = '20-24' THEN 6
        WHEN a.datim_agegroup = '25-29' THEN 7
        WHEN a.datim_agegroup = '30-34' THEN 8
        WHEN a.datim_agegroup = '35-39' THEN 9
        WHEN a.datim_agegroup = '40-44' THEN 10
        WHEN a.datim_agegroup = '45-49' THEN 11
        WHEN a.datim_agegroup = '50-54' THEN 12
        WHEN a.datim_agegroup = '55-59' THEN 13
        WHEN a.datim_agegroup = '60-64' THEN 14
        WHEN a.datim_agegroup = '65+' THEN 15
    END
WHERE a.datim_agegroup IS NOT NULL;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_dim_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_dim_agegroup;


~-~-
CREATE PROCEDURE sp_icare_dim_agegroup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_dim_agegroup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_dim_agegroup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_dim_agegroup_create();
CALL sp_icare_dim_agegroup_insert();
CALL sp_icare_dim_agegroup_update();
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs_create;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_z_encounter_obs_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_z_encounter_obs_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE icare_z_encounter_obs
(
    obs_id                  INT           NOT NULL UNIQUE PRIMARY KEY,
    encounter_id            INT           NULL,
    visit_id                INT           NULL,
    person_id               INT           NOT NULL,
    order_id                INT           NULL,
    encounter_datetime      DATETIME      NOT NULL,
    obs_datetime            DATETIME      NOT NULL,
    location_id             INT           NULL,
    obs_group_id            INT           NULL,
    obs_question_concept_id INT DEFAULT 0 NOT NULL,
    obs_value_text          TEXT          NULL,
    obs_value_numeric       DOUBLE        NULL,
    obs_value_boolean       BOOLEAN       NULL,
    obs_value_coded         INT           NULL,
    obs_value_datetime      DATETIME      NULL,
    obs_value_complex       VARCHAR(1000) NULL,
    obs_value_drug          INT           NULL,
    obs_question_uuid       CHAR(38),
    obs_answer_uuid         CHAR(38),
    obs_value_coded_uuid    CHAR(38),
    encounter_type_uuid     CHAR(38),
    status                  VARCHAR(16)   NOT NULL,
    previous_version        INT           NULL,
    date_created            DATETIME      NOT NULL,
    date_voided             DATETIME      NULL,
    voided                  TINYINT(1)    NOT NULL,
    voided_by               INT           NULL,
    void_reason             VARCHAR(255)  NULL,
    incremental_record      INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX icare_idx_encounter_id (encounter_id),
    INDEX icare_idx_visit_id (visit_id),
    INDEX icare_idx_person_id (person_id),
    INDEX icare_idx_encounter_datetime (encounter_datetime),
    INDEX icare_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX icare_idx_obs_question_concept_id (obs_question_concept_id),
    INDEX icare_idx_obs_value_coded (obs_value_coded),
    INDEX icare_idx_obs_value_coded_uuid (obs_value_coded_uuid),
    INDEX icare_idx_obs_question_uuid (obs_question_uuid),
    INDEX icare_idx_status (status),
    INDEX icare_idx_voided (voided),
    INDEX icare_idx_date_voided (date_voided),
    INDEX icare_idx_order_id (order_id),
    INDEX icare_idx_previous_version (previous_version),
    INDEX icare_idx_obs_group_id (obs_group_id),
    INDEX icare_idx_incremental_record (incremental_record),
    INDEX idx_encounter_person_datetime (encounter_id, person_id, encounter_datetime)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs_insert;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs_insert()
BEGIN
    DECLARE batch_size INT DEFAULT 1000000; -- 1m batch size
    DECLARE batch_last_obs_id INT DEFAULT 0;
    DECLARE last_obs_id INT;

    CREATE TEMPORARY TABLE IF NOT EXISTS icare_temp_obs_data AS
    SELECT o.obs_id,
           o.encounter_id,
           e.visit_id,
           o.person_id,
           o.order_id,
           e.encounter_datetime,
           o.obs_datetime,
           o.location_id,
           o.obs_group_id,
           o.concept_id     AS obs_question_concept_id,
           o.value_text     AS obs_value_text,
           o.value_numeric  AS obs_value_numeric,
           o.value_coded    AS obs_value_coded,
           o.value_datetime AS obs_value_datetime,
           o.value_complex  AS obs_value_complex,
           o.value_drug     AS obs_value_drug,
           md.concept_uuid  AS obs_question_uuid,
           NULL             AS obs_answer_uuid,
           NULL             AS obs_value_coded_uuid,
           e.encounter_type_uuid,
           o.status,
           o.previous_version,
           o.date_created,
           o.date_voided,
           o.voided,
           o.voided_by,
           o.void_reason
    FROM icare_source_db.obs o
             INNER JOIN icare_dim_encounter e ON o.encounter_id = e.encounter_id
             INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                         FROM icare_concept_metadata) md ON o.concept_id = md.concept_id
    WHERE o.encounter_id IS NOT NULL;

    CREATE INDEX idx_obs_id ON icare_temp_obs_data (obs_id);

    SELECT MAX(obs_id) INTO last_obs_id FROM icare_temp_obs_data;

    WHILE batch_last_obs_id < last_obs_id
        DO
            INSERT INTO icare_z_encounter_obs (obs_id,
                                               encounter_id,
                                               visit_id,
                                               person_id,
                                               order_id,
                                               encounter_datetime,
                                               obs_datetime,
                                               location_id,
                                               obs_group_id,
                                               obs_question_concept_id,
                                               obs_value_text,
                                               obs_value_numeric,
                                               obs_value_coded,
                                               obs_value_datetime,
                                               obs_value_complex,
                                               obs_value_drug,
                                               obs_question_uuid,
                                               obs_answer_uuid,
                                               obs_value_coded_uuid,
                                               encounter_type_uuid,
                                               status,
                                               previous_version,
                                               date_created,
                                               date_voided,
                                               voided,
                                               voided_by,
                                               void_reason)
            SELECT obs_id,
                   encounter_id,
                   visit_id,
                   person_id,
                   order_id,
                   encounter_datetime,
                   obs_datetime,
                   location_id,
                   obs_group_id,
                   obs_question_concept_id,
                   obs_value_text,
                   obs_value_numeric,
                   obs_value_coded,
                   obs_value_datetime,
                   obs_value_complex,
                   obs_value_drug,
                   obs_question_uuid,
                   obs_answer_uuid,
                   obs_value_coded_uuid,
                   encounter_type_uuid,
                   status,
                   previous_version,
                   date_created,
                   date_voided,
                   voided,
                   voided_by,
                   void_reason
            FROM icare_temp_obs_data
            WHERE obs_id > batch_last_obs_id
            ORDER BY obs_id ASC
            LIMIT batch_size;

            SELECT MAX(obs_id)
            INTO batch_last_obs_id
            FROM icare_z_encounter_obs
            LIMIT 1;

        END WHILE;

    DROP TEMPORARY TABLE IF EXISTS icare_temp_obs_data;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs_update;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs_update()
BEGIN
    DECLARE total_records INT;
    DECLARE batch_size INT DEFAULT 1000000; -- 1 million batches
    DECLARE icare_offset INT DEFAULT 0;

    SELECT COUNT(*)
    INTO total_records
    FROM icare_z_encounter_obs;
    CREATE
        TEMPORARY TABLE icare_temp_value_coded_values
        CHARSET = UTF8MB4 AS
    SELECT m.concept_id AS concept_id,
           m.uuid       AS concept_uuid,
           m.name       AS concept_name
    FROM icare_dim_concept m
    WHERE concept_id in (SELECT DISTINCT obs_value_coded
                         FROM icare_z_encounter_obs
                         WHERE obs_value_coded IS NOT NULL);

    CREATE INDEX icare_idx_concept_id ON icare_temp_value_coded_values (concept_id);

    -- update obs_value_coded (UUIDs & Concept value names)
    WHILE icare_offset < total_records
        DO
            UPDATE icare_z_encounter_obs z
                JOIN (SELECT encounter_id
                      FROM icare_z_encounter_obs
                      ORDER BY encounter_id
                      LIMIT batch_size OFFSET icare_offset) AS filter
                ON filter.encounter_id = z.encounter_id
                INNER JOIN icare_temp_value_coded_values mtv
                ON z.obs_value_coded = mtv.concept_id
            SET z.obs_value_text       = mtv.concept_name,
                z.obs_value_coded_uuid = mtv.concept_uuid
            WHERE z.obs_value_coded IS NOT NULL;

            SET icare_offset = icare_offset + batch_size;
        END WHILE;

    -- update column obs_value_boolean (Concept values)
    UPDATE icare_z_encounter_obs z
    SET obs_value_boolean =
            CASE
                WHEN obs_value_text IN ('FALSE', 'No') THEN 0
                WHEN obs_value_text IN ('TRUE', 'Yes') THEN 1
                ELSE NULL
                END
    WHERE z.obs_value_coded IS NOT NULL
      AND obs_question_concept_id in
          (SELECT DISTINCT concept_id
           FROM icare_dim_concept c
           WHERE c.datatype = 'Boolean');

    DROP TEMPORARY TABLE IF EXISTS icare_temp_value_coded_values;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_z_encounter_obs', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_z_encounter_obs', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_z_encounter_obs_create();
CALL sp_icare_z_encounter_obs_insert();
CALL sp_icare_z_encounter_obs_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs_incremental_insert;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_z_encounter_obs_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_z_encounter_obs_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert into icare_z_encounter_obs
INSERT INTO icare_z_encounter_obs (obs_id,
                                   encounter_id,
                                   visit_id,
                                   person_id,
                                   order_id,
                                   encounter_datetime,
                                   obs_datetime,
                                   location_id,
                                   obs_group_id,
                                   obs_question_concept_id,
                                   obs_value_text,
                                   obs_value_numeric,
                                   obs_value_coded,
                                   obs_value_datetime,
                                   obs_value_complex,
                                   obs_value_drug,
                                   obs_question_uuid,
                                   obs_answer_uuid,
                                   obs_value_coded_uuid,
                                   encounter_type_uuid,
                                   status,
                                   previous_version,
                                   date_created,
                                   date_voided,
                                   voided,
                                   voided_by,
                                   void_reason,
                                   incremental_record)
SELECT o.obs_id,
       o.encounter_id,
       e.visit_id,
       o.person_id,
       o.order_id,
       e.encounter_datetime,
       o.obs_datetime,
       o.location_id,
       o.obs_group_id,
       o.concept_id     AS obs_question_concept_id,
       o.value_text     AS obs_value_text,
       o.value_numeric  AS obs_value_numeric,
       o.value_coded    AS obs_value_coded,
       o.value_datetime AS obs_value_datetime,
       o.value_complex  AS obs_value_complex,
       o.value_drug     AS obs_value_drug,
       md.concept_uuid  AS obs_question_uuid,
       NULL             AS obs_answer_uuid,
       NULL             AS obs_value_coded_uuid,
       e.encounter_type_uuid,
       o.status,
       o.previous_version,
       o.date_created,
       o.date_voided,
       o.voided,
       o.voided_by,
       o.void_reason,
       1
FROM icare_source_db.obs o
         INNER JOIN icare_etl_incremental_columns_index_new ic ON o.obs_id = ic.incremental_table_pkey
         INNER JOIN icare_dim_encounter e ON o.encounter_id = e.encounter_id
         INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                     FROM icare_concept_metadata) md ON o.concept_id = md.concept_id
WHERE o.encounter_id IS NOT NULL;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs_incremental_update;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_z_encounter_obs_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_z_encounter_obs_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records

-- Update voided Obs (FINAL & AMENDED pair obs are incremental 1 though we shall not consider them in incremental flattening)
UPDATE icare_z_encounter_obs z
    INNER JOIN icare_etl_incremental_columns_index_modified im
    ON z.obs_id = im.incremental_table_pkey
    INNER JOIN icare_source_db.obs o
    ON z.obs_id = o.obs_id
SET z.encounter_id            = o.encounter_id,
    z.person_id               = o.person_id,
    z.order_id                = o.order_id,
    z.obs_datetime            = o.obs_datetime,
    z.location_id             = o.location_id,
    z.obs_group_id            = o.obs_group_id,
    z.obs_question_concept_id = o.concept_id,
    z.obs_value_text          = o.value_text,
    z.obs_value_numeric       = o.value_numeric,
    z.obs_value_coded         = o.value_coded,
    z.obs_value_datetime      = o.value_datetime,
    z.obs_value_complex       = o.value_complex,
    z.obs_value_drug          = o.value_drug,
    -- z.encounter_type_uuid     = o.encounter_type_uuid,
    z.status                  = o.status,
    z.previous_version        = o.previous_version,
    -- z.row_num            = o.row_num,
    z.date_created            = o.date_created,
    z.voided                  = o.voided,
    z.voided_by               = o.voided_by,
    z.date_voided             = o.date_voided,
    z.void_reason             = o.void_reason,
    z.incremental_record      = 1
WHERE im.incremental_table_pkey > 1;

-- update obs_value_coded (UUIDs & Concept value names) for only NEW Obs (not voided)
UPDATE icare_z_encounter_obs z
    INNER JOIN icare_dim_concept c
    ON z.obs_value_coded = c.concept_id
SET z.obs_value_text       = c.name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.incremental_record = 1
  AND z.obs_value_coded IS NOT NULL;

-- update column obs_value_boolean (Concept values) for only NEW Obs (not voided)
UPDATE icare_z_encounter_obs z
SET obs_value_boolean =
        CASE
            WHEN obs_value_text IN ('FALSE', 'No') THEN 0
            WHEN obs_value_text IN ('TRUE', 'Yes') THEN 1
            ELSE NULL
            END
WHERE z.incremental_record = 1
  AND z.obs_value_coded IS NOT NULL
  AND obs_question_concept_id in
      (SELECT DISTINCT concept_id
       FROM icare_dim_concept c
       WHERE c.datatype = 'Boolean');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_z_encounter_obs_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_z_encounter_obs_incremental;


~-~-
CREATE PROCEDURE sp_icare_z_encounter_obs_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_icare_etl_error_log_insert('sp_icare_z_encounter_obs_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _icare_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_icare_z_encounter_obs_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _icare_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_icare_etl_incremental_columns_index('obs', 'icare_z_encounter_obs');
CALL sp_icare_z_encounter_obs_incremental_insert();
CALL sp_icare_z_encounter_obs_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_data_processing_drop_and_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_data_processing_drop_and_flatten;


~-~-
CREATE PROCEDURE sp_icare_data_processing_drop_and_flatten()

BEGIN

    CALL sp_icare_system_drop_all_tables();

    CALL sp_icare_dim_agegroup;

    CALL sp_icare_dim_location;

    CALL sp_icare_dim_patient_identifier_type;

    CALL sp_icare_dim_concept_datatype;

    CALL sp_icare_dim_concept_name;

    CALL sp_icare_dim_concept;

    CALL sp_icare_dim_concept_answer;

    CALL sp_icare_dim_encounter_type;

    CALL sp_icare_flat_table_config;

    CALL sp_icare_concept_metadata;

    CALL sp_icare_dim_report_definition;

    CALL sp_icare_dim_encounter;

    CALL sp_icare_dim_person_name;

    CALL sp_icare_dim_person;

    CALL sp_icare_dim_person_attribute_type;

    CALL sp_icare_dim_person_attribute;

    CALL sp_icare_dim_person_address;

    CALL sp_icare_dim_user;

    CALL sp_icare_dim_relationship;

    CALL sp_icare_dim_patient_identifier;

    CALL sp_icare_dim_orders;

    CALL sp_icare_z_encounter_obs;

    CALL sp_icare_obs_group;

    CALL sp_icare_flat_encounter_table_create_all;

    CALL sp_icare_flat_encounter_table_insert_all;

    CALL sp_icare_flat_encounter_obs_group_table_create_all;

    CALL sp_icare_flat_encounter_obs_group_table_insert_all;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_data_processing_increment_and_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_icare_data_processing_increment_and_flatten;


~-~-
CREATE PROCEDURE sp_icare_data_processing_increment_and_flatten()

BEGIN

    CALL sp_icare_dim_location_incremental;

    CALL sp_icare_dim_patient_identifier_type_incremental;

    CALL sp_icare_dim_concept_datatype_incremental;

    CALL sp_icare_dim_concept_name_incremental;

    CALL sp_icare_dim_concept_incremental;

    CALL sp_icare_dim_concept_answer_incremental;

    CALL sp_icare_dim_encounter_type_incremental;

    CALL sp_icare_flat_table_config_incremental;

    CALL sp_icare_concept_metadata_incremental;

    CALL sp_icare_dim_encounter_incremental;

    CALL sp_icare_dim_person_name_incremental;

    CALL sp_icare_dim_person_incremental;

    CALL sp_icare_dim_person_attribute_type_incremental;

    CALL sp_icare_dim_person_attribute_incremental;

    CALL sp_icare_dim_person_address_incremental;

    CALL sp_icare_dim_user_incremental;

    CALL sp_icare_dim_relationship_incremental;

    CALL sp_icare_dim_patient_identifier_incremental;

    CALL sp_icare_dim_orders_incremental;

    -- incremental inserts into the icare_z_encounter_obs table only
    CALL sp_icare_z_encounter_obs_incremental;

    CALL sp_icare_flat_table_incremental_create_all;

    -- create and insert into flat tables whose columns or table names have been modified/added (determined by json_data hash)
    CALL sp_icare_flat_table_incremental_insert_all;

    -- insert from icare_z_encounter_obs into the flat table OBS that are either MODIFIED or CREATED/NEW
    -- (Deletes and inserts an entire Encounter (by id) if one of the obs is modified)
    CALL sp_icare_flat_table_incremental_update_encounter;

    CALL sp_icare_reset_incremental_update_flag_all;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_icare_data_processing_etl  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_icare_data_processing_etl;

~-~-
CREATE PROCEDURE sp_icare_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- Call the implementer ETL process

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------------  Setup the MambaETL Scheduler  ---------------------------------
-- ---------------------------------------------------------------------------------------------


-- Enable the event etl_scheduler
SET GLOBAL event_scheduler = ON;

~-~-

-- Drop/Create the Event responsible for firing up the ETL process
DROP EVENT IF EXISTS _icare_etl_scheduler_event;

~-~-

-- Drop/Create the Event responsible for maintaining event logs at a max. 20 elements
DROP EVENT IF EXISTS _icare_etl_scheduler_trim_log_event;

~-~-

-- Setup ETL configurations
CALL sp_icare_etl_setup(?, ?, ?, ?, ?, ?, ?);

-- pass them from the runtime properties file

~-~-

CREATE EVENT IF NOT EXISTS _icare_etl_scheduler_event
    ON SCHEDULE EVERY ? SECOND
        STARTS CURRENT_TIMESTAMP
    DO CALL sp_icare_etl_schedule();

~-~-

-- Setup a trigger that trims record off _icare_etl_schedule to just leave 20 latest records.
-- to avoid the table growing too big

 CREATE EVENT IF NOT EXISTS _icare_etl_scheduler_trim_log_event
    ON SCHEDULE EVERY 3 HOUR
        STARTS CURRENT_TIMESTAMP
    DO CALL sp_icare_etl_schedule_trim_log_event();

 ~-~-

