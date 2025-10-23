--  Copyright 2025 Jonas G Mwambimbi
--  Licensed under the Apache License, Version 2.0 (the "License");
--     you may not use this file except in compliance with the License.
--     You may obtain a copy of the License at

--         http://www.apache.org/licenses/LICENSE-2.0

CREATE TABLE IF NOT EXISTS chacc_pipeline_task_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_history_id INT NOT NULL COMMENT 'Reference to chacc_pipeline_history.id',
    task_name VARCHAR(255) NOT NULL COMMENT 'Name of the task that executed',
    task_type VARCHAR(50) NOT NULL COMMENT 'Type of task: schema, procedure, extract_load, etc.',
    status VARCHAR(50) NOT NULL COMMENT 'Task status: running, completed, failed, interrupted',
    start_time DATETIME COMMENT 'When the task started',
    end_time DATETIME COMMENT 'When the task completed',
    duration_seconds DECIMAL(10,2) COMMENT 'Duration of the task in seconds',
    error_message TEXT COMMENT 'Error message if task failed',
    records_processed INT DEFAULT 0 COMMENT 'Number of records processed (for data tasks)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_history_id) REFERENCES chacc_pipeline_history(id) ON DELETE CASCADE,
    INDEX idx_pipeline (pipeline_history_id),
    INDEX idx_task (task_name),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tracks individual task executions within pipelines';