CREATE TABLE IF NOT EXISTS icare_analytics.etl_process_history (
    process_name VARCHAR(100) PRIMARY KEY,
    last_run_date DATETIME,
    status VARCHAR(20)
);