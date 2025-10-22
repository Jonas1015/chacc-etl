# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0


import luigi
import os
import pymysql
from datetime import datetime
from tasks.base_tasks import TargetDatabaseTask
from utils import log_task_start, log_task_complete, log_task_error, execute_query
from config import DATA_DIR, TARGET_DB_CONFIG

def read_sql_file(filepath):
    """Read SQL content from a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read().strip()

def update_task_status(conn, task_name, status, error_message=None):
    """Update task status in chacc_etl_metadata table."""
    try:
        sql = """
            INSERT INTO chacc_etl_metadata (table_name, status, error_message, last_update_timestamp, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                error_message = VALUES(error_message),
                last_update_timestamp = VALUES(last_update_timestamp),
                updated_at = VALUES(updated_at)
        """
        execute_query(conn, sql, (task_name, status, error_message, datetime.now(), datetime.now()))
    except Exception as e:
        print(f"Could not update task status for {task_name}: {e}")

class BaseSQLTask(TargetDatabaseTask):
    """Base class for SQL execution tasks."""

    def update_status(self, status, error_message=None):
        """Update task execution status."""
        try:
            with self.get_db_connection() as conn:
                update_task_status(conn, self.__class__.__name__, status, error_message)
        except:
            pass

    def run_sql_file(self, sql_file_path, **kwargs):
        """Execute SQL from a file with optional string formatting."""
        sql = read_sql_file(sql_file_path)
        if kwargs:
            sql = sql.format(**kwargs)
        with self.get_db_connection() as conn:
            execute_query(conn, sql)
