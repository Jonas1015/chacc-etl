--  Copyright 2025 Jonas G Mwambimbi
--  Licensed under the Apache License, Version 2.0 (the "License");
--     you may not use this file except in compliance with the License.
--     You may obtain a copy of the License at

--         http://www.apache.org/licenses/LICENSE-2.0

CREATE TABLE IF NOT EXISTS chacc_pipeline_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    action VARCHAR(50) NOT NULL COMMENT 'Pipeline action (full_refresh, incremental, scheduled_incremental, etc.)',
    pipeline_type VARCHAR(100) NOT NULL COMMENT 'Human readable pipeline type',
    start_time DATETIME NOT NULL COMMENT 'When the pipeline started',
    end_time DATETIME NULL COMMENT 'When the pipeline completed',
    duration_seconds DECIMAL(10,2) NULL COMMENT 'How long the pipeline took',
    success BOOLEAN NULL COMMENT 'Whether the pipeline succeeded',
    status VARCHAR(20) NOT NULL DEFAULT 'completed' COMMENT 'Pipeline status (pending, done, completed, failed, interrupted)',
    result TEXT COMMENT 'Pipeline result or error message',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_action (action),
    INDEX idx_success (success),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time),
    INDEX idx_pipeline_type (pipeline_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tracks all pipeline execution history';

CREATE TABLE IF NOT EXISTS chacc_pipeline_task_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_history_id INT NOT NULL COMMENT 'Reference to chacc_pipeline_history.id',
    task_name VARCHAR(255) NOT NULL COMMENT 'Name of the task that executed',
    task_type VARCHAR(50) NOT NULL COMMENT 'Type of task: schema, procedure, extract_load, etc.',
    status VARCHAR(50) NOT NULL COMMENT 'Task status: running, completed, failed, interrupted',
    start_time DATETIME,
    end_time DATETIME,
    duration_seconds DECIMAL(10,2),
    error_message TEXT,
    records_processed INT DEFAULT 0 COMMENT 'Number of records processed (for data tasks)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (pipeline_history_id) REFERENCES chacc_pipeline_history(id) ON DELETE CASCADE,
    INDEX idx_pipeline (pipeline_history_id),
    INDEX idx_task (task_name),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tracks individual task executions within pipelines';