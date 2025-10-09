"""
Dynamic task factory that creates Luigi tasks from JSON configuration.
Creates all tasks dynamically from task_definitions.json.
"""

import os
import json
import luigi
from tasks.base_tasks import TargetDatabaseTask
from utils import log_task_start, log_task_complete, log_task_error, execute_query
from datetime import datetime
from config import TARGET_DB_CONFIG, SOURCE_DB_CONFIG, DATA_DIR

def load_task_definitions():
    """Load task definitions from JSON configuration files in config/tasks/ folder."""
    tasks_dir = os.path.join(os.path.dirname(__file__), '..', 'config', 'tasks')
    tasks = {}
    if os.path.exists(tasks_dir):
        for file in sorted(os.listdir(tasks_dir)):
            if file.endswith('.json'):
                file_path = os.path.join(tasks_dir, file)
                with open(file_path, 'r') as f:
                    config = json.load(f)
                    if 'tasks' in config:
                        tasks.update(config['tasks'])
    return tasks, {}

def update_task_status(conn, task_name, status, error_message=None):
    """Update task status in etl_metadata table."""
    try:
        sql = """
            INSERT INTO etl_metadata (table_name, status, error_message, last_update_timestamp, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                error_message = VALUES(error_message),
                last_update_timestamp = VALUES(last_update_timestamp),
                updated_at = VALUES(updated_at)
        """
        execute_query(conn, sql, (task_name, status, error_message, datetime.now(), datetime.now()))
    except Exception as e:
        if "doesn't exist" not in str(e):
            print(f"Could not update task status for {task_name}: {e}")

def get_sql_content(task_config):
    """Get SQL content - either from direct string or file path."""
    if 'sql' in task_config:
        sql_value = task_config['sql']
        if isinstance(sql_value, str) and ('/' in sql_value or sql_value.startswith('sql/')):
            sql_file_path = os.path.join(os.path.dirname(__file__), '..', sql_value)
            if os.path.exists(sql_file_path):
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            else:
                raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        else:
            return sql_value
    return ''

def create_dynamic_tasks():
    """Create Luigi task classes dynamically from JSON configuration."""
    task_definitions, dependencies_config = load_task_definitions()

    dynamic_tasks = {}

    for task_name, task_config in task_definitions.items():
        task_type = task_config.get('type')

        if task_type == 'schema':
            class DynamicSchemaTask(TargetDatabaseTask):
                sql = luigi.Parameter()
                task_config = luigi.Parameter()

                def update_status(self, status, error_message=None):
                    try:
                        with self.get_db_connection() as conn:
                            update_task_status(conn, self.__class__.__name__, status, error_message)
                    except:
                        pass

                def output(self):
                    task_name = self.__class__.__name__
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name.lower()}_completed.txt'))

                def run(self):
                    try:
                        log_task_start(self)
                        self.update_status('running')

                        sql = self.sql
                        print(f"[{self.__class__.__name__}] Executing SQL: {sql}")
                        if 'CREATE DATABASE' in sql.upper():
                            print(f"[{self.__class__.__name__}] Using MySQL system connection for database creation")
                            from utils.db_utils import get_mysql_db_connection
                            with get_mysql_db_connection() as conn:
                                execute_query(conn, sql)
                        else:
                            print(f"[{self.__class__.__name__}] Using target database connection")
                            with self.get_db_connection() as conn:
                                execute_query(conn, sql)

                        print(f"[{self.__class__.__name__}] SQL executed successfully")
                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully\n')

                        self.update_status('completed')
                        log_task_complete(self)
                    except Exception as e:
                        print(f"[{self.__class__.__name__}] ERROR: {str(e)}")
                        self.update_status('failed', str(e))
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicSchemaTask,), {
                'sql': get_sql_content(task_config),
                'task_config': task_config
            })

        elif task_type == 'procedure':
            class DynamicProcedureTask(TargetDatabaseTask):
                sql = luigi.Parameter()
                task_config = luigi.Parameter()

                def update_status(self, status, error_message=None):
                    try:
                        with self.get_db_connection() as conn:
                            update_task_status(conn, self.__class__.__name__, status, error_message)
                    except:
                        pass

                def output(self):
                    task_name = self.__class__.__name__
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name.lower()}_completed.txt'))

                def run(self):
                    try:
                        log_task_start(self)
                        self.update_status('running')

                        sql = self.sql
                        with self.get_db_connection() as conn:
                            import re
                            sql = re.sub(r'DELIMITER\s+//', '', sql)
                            sql = re.sub(r'DELIMITER\s+;', '', sql)
                            statements = sql.split('//')
                            with conn.cursor() as cursor:
                                for statement in statements:
                                    statement = statement.strip()
                                    if statement:
                                        cursor.execute(statement)
                            conn.commit()

                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully\n')

                        self.update_status('completed')
                        log_task_complete(self)
                    except Exception as e:
                        self.update_status('failed', str(e))
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicProcedureTask,), {
                'sql': get_sql_content(task_config),
                'task_config': task_config
            })

        elif task_type == 'extract_load':
            class DynamicExtractLoadTask(TargetDatabaseTask):
                task_config = luigi.Parameter()
                incremental = luigi.BoolParameter(default=False)
                last_updated = luigi.DateParameter(default=None)

                def requires(self):
                    return []

                def output(self):
                    task_name_lower = self.__class__.__name__.lower()
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name_lower}_completed.txt'))

                def run(self):
                    try:
                        log_task_start(self)

                        target_table = self.task_config.get('target_table')
                        incremental_field = self.task_config.get('incremental_field')

                        query = get_sql_content({'sql': self.task_config.get('query')})
                        full_load_sql = get_sql_content({'sql': self.task_config.get('full_load_sql')})
                        incremental_sql = get_sql_content({'sql': self.task_config.get('incremental_sql')})

                        if not query:
                            raise ValueError(f"No query defined for {self.__class__.__name__}")
                        if not full_load_sql:
                            raise ValueError(f"No full_load_sql defined for {self.__class__.__name__}")

                        params = None
                        if self.incremental and incremental_field and self.last_updated:
                            if '%s' in query:
                                params = (self.last_updated,)
                            else:
                                query += f" WHERE {incremental_field} > %s ORDER BY {incremental_field}"
                                params = (self.last_updated,)

                        from utils.db_utils import get_source_db_connection
                        with get_source_db_connection() as source_conn:
                            from utils.db_utils import execute_query
                            results = execute_query(source_conn, query, params, fetch=True)

                        if not results:
                            self.logger.info("No data to load")
                            with self.output().open('w') as f:
                                f.write(f'{self.__class__.__name__} completed successfully (no data)\n')
                            return

                        sql = incremental_sql if (self.incremental and incremental_sql) else full_load_sql

                        data_tuples = []
                        for row in results:
                            if isinstance(row, dict):
                                data_tuples.append(tuple(row.values()))
                            elif isinstance(row, (list, tuple)):
                                data_tuples.append(tuple(row))
                            else:
                                data_tuples.append((row,))

                        with self.get_db_connection() as target_conn:
                            if not self.incremental:
                                print(f"[{self.__class__.__name__}] Truncating table {target_table}")
                                with target_conn.cursor() as cursor:
                                    cursor.execute(f"TRUNCATE TABLE {target_table}")
                            print(f"[{self.__class__.__name__}] Inserting {len(data_tuples)} records into {target_table}")
                            with target_conn.cursor() as cursor:
                                cursor.executemany(sql, data_tuples)
                            target_conn.commit()

                        try:
                            metadata_sql = """
                                INSERT INTO etl_metadata (table_name, record_count, status, updated_at)
                                VALUES (%s, %s, 'completed', NOW())
                                ON DUPLICATE KEY UPDATE
                                    record_count = VALUES(record_count),
                                    status = 'completed',
                                    updated_at = NOW()
                            """
                            execute_query(target_conn, metadata_sql, (target_table, len(results)))
                        except:
                            pass

                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully: {len(results)} records\n')

                        log_task_complete(self)
                    except Exception as e:
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicExtractLoadTask,), {
                'task_config': task_config
            })

        elif task_type == 'load':
            class DynamicLoadTask(TargetDatabaseTask):
                task_config = luigi.Parameter()
                incremental = luigi.BoolParameter(default=False)
                last_updated = luigi.DateParameter(default=None)

                def requires(self):
                    extract_task_name = task_name.replace('Load', 'Extract')
                    return [dynamic_tasks.get(extract_task_name, type('DummyTask', (), {}))()]

                def output(self):
                    task_name_lower = self.__class__.__name__.lower()
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name_lower}_completed.txt'))

                def run(self):
                    try:
                        log_task_start(self)

                        source_file = self.task_config['source_file']
                        source_path = os.path.join(DATA_DIR, source_file)

                        if not os.path.exists(source_path):
                            self.logger.info(f"No data file {source_file} found, skipping load")
                            with self.output().open('w') as f:
                                f.write(f'{self.__class__.__name__} completed successfully (no data)\n')
                            return

                        with open(source_path, 'r') as f:
                            data = json.load(f)

                        if not data:
                            self.logger.info("No data to load")
                            with self.output().open('w') as f:
                                f.write(f'{self.__class__.__name__} completed successfully (no data)\n')
                            return

                        target_table = self.task_config['target_table']
                        sql = self.task_config['incremental_sql'] if self.incremental else self.task_config['full_load_sql']

                        data_tuples = []
                        for row in data:
                            if isinstance(row, dict):
                                data_tuples.append(tuple(row.values()))
                            elif isinstance(row, (list, tuple)):
                                data_tuples.append(tuple(row))
                            else:
                                data_tuples.append((row,))

                        with self.get_db_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.executemany(sql, data_tuples)
                            conn.commit()

                        try:
                            metadata_sql = """
                                INSERT INTO etl_metadata (table_name, record_count, status, updated_at)
                                VALUES (%s, %s, 'completed', NOW())
                                ON DUPLICATE KEY UPDATE
                                    record_count = VALUES(record_count),
                                    status = 'completed',
                                    updated_at = NOW()
                            """
                            execute_query(conn, metadata_sql, (target_table, len(data)))
                        except:
                            pass

                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully: {len(data)} records\n')

                        log_task_complete(self)
                    except Exception as e:
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicLoadTask,), {
                'task_config': task_config
            })

        elif task_type == 'flattened':
            class DynamicFlattenedTask(TargetDatabaseTask):
                task_config = luigi.Parameter()

                def output(self):
                    task_name_lower = self.__class__.__name__.lower()
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name_lower}_created.txt'))

                def run(self):
                    try:
                        log_task_start(self)

                        procedure_call = self.task_config['procedure_call']
                        with self.get_db_connection() as conn:
                            execute_query(conn, procedure_call)

                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully\n')

                        log_task_complete(self)
                    except Exception as e:
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicFlattenedTask,), {
                'task_config': task_config
            })

        elif task_type == 'summary':
            class DynamicSummaryTask(TargetDatabaseTask):
                task_config = luigi.Parameter()

                def output(self):
                    task_name_lower = self.__class__.__name__.lower()
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name_lower}_created.txt'))

                def run(self):
                    try:
                        log_task_start(self)
                        print(f"[{self.__class__.__name__}] Starting summary table creation")

                        sql = getattr(self, 'sql', self.task_config.get('sql', ''))
                        print(f"[{self.__class__.__name__}] Executing SQL: {sql[:100]}...")
                        with self.get_db_connection() as conn:
                            statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
                            for stmt in statements:
                                if stmt:
                                    print(f"[{self.__class__.__name__}] Executing statement: {stmt[:50]}...")
                                    execute_query(conn, stmt)
                            print(f"[{self.__class__.__name__}] Table creation completed")

                            indexes = self.task_config.get('indexes', [])
                            for i, index_sql in enumerate(indexes):
                                print(f"[{self.__class__.__name__}] Creating index {i+1}/{len(indexes)}")
                                try:
                                    execute_query(conn, index_sql)
                                except Exception as e:
                                    if "Duplicate key name" not in str(e):
                                        raise
                            print(f"[{self.__class__.__name__}] All indexes created")

                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully\n')

                        print(f"[{self.__class__.__name__}] Task completed successfully")
                        log_task_complete(self)
                    except Exception as e:
                        print(f"[{self.__class__.__name__}] ERROR: {str(e)}")
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicSummaryTask,), {
                'task_config': task_config
            })

            if 'sql' in task_config:
                task_class.sql = get_sql_content(task_config)

        else:
            class DynamicTask(TargetDatabaseTask):
                task_config = luigi.Parameter()

                def output(self):
                    task_name_lower = self.__class__.__name__.lower()
                    return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name_lower}_completed.txt'))

                def run(self):
                    try:
                        log_task_start(self)

                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully\n')

                        log_task_complete(self)
                    except Exception as e:
                        log_task_error(self, e)
                        raise

            task_class = type(task_name, (DynamicTask,), {
                'task_config': task_config
            })

        dynamic_tasks[task_name] = task_class

    for task_name, task_class in dynamic_tasks.items():
        task_config = task_definitions.get(task_name, {})
        deps = task_config.get('dependencies', [])
        task_class.requires = lambda self, deps=deps, tasks=dynamic_tasks: [tasks[dep]() for dep in deps if dep in tasks]

    return dynamic_tasks