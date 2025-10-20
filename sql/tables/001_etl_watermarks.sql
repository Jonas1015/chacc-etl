--  Copyright 2025 Jonas G Mwambimbi
--  Licensed under the Apache License, Version 2.0 (the "License");
--     you may not use this file except in compliance with the License.
--     You may obtain a copy of the License at

--         http://www.apache.org/licenses/LICENSE-2.0

CREATE TABLE IF NOT EXISTS etl_watermarks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    incremental_field VARCHAR(100) DEFAULT 'id',
    last_processed_value VARCHAR(255),
    last_processed_timestamp DATETIME,
    batch_size INT DEFAULT 1000,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_target (source_table, target_table),
    INDEX idx_active (is_active),
    INDEX idx_last_processed (last_processed_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4