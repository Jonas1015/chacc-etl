--  Copyright 2025 Jonas G Mwambimbi
--  Licensed under the Apache License, Version 2.0 (the "License");
--     you may not use this file except in compliance with the License.
--     You may obtain a copy of the License at

--         http://www.apache.org/licenses/LICENSE-2.0

CREATE TABLE IF NOT EXISTS chacc_etl_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    last_update_timestamp DATETIME,
    record_count INT DEFAULT 0,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_table (table_name),
    INDEX idx_status (status),
    INDEX idx_last_update (last_update_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4