# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
History Service Module

Handles pipeline execution history tracking and storage in database.
"""

import time
from datetime import datetime
import pymysql

try:
    from utils.db_utils import get_target_db_connection
    HAS_DB_UTILS = True
except ImportError:
    HAS_DB_UTILS = False
    get_target_db_connection = None


def log_pipeline_execution(action, start_time, end_time, success, result="", status="completed"):
    """Log a pipeline execution to database."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, skipping history logging")
            return None

        with get_target_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chacc_pipeline_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    action VARCHAR(50) NOT NULL,
                    pipeline_type VARCHAR(100) NOT NULL,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME,
                    duration_seconds DECIMAL(10,2),
                    success BOOLEAN,
                    status VARCHAR(20) NOT NULL DEFAULT 'completed',
                    result TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_action (action),
                    INDEX idx_success (success),
                    INDEX idx_status (status),
                    INDEX idx_start_time (start_time),
                    INDEX idx_pipeline_type (pipeline_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            end_time_val = datetime.fromtimestamp(end_time) if end_time else None
            duration_val = round(end_time - start_time, 2) if end_time else None
            success_val = success if status == 'completed' else None

            cursor.execute("""
                INSERT INTO chacc_pipeline_history
                (action, pipeline_type, start_time, end_time, duration_seconds, success, status, result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                action,
                action.replace('_', ' ').title(),
                datetime.fromtimestamp(start_time),
                end_time_val,
                duration_val,
                success_val,
                status,
                result[:1000] if result else None
            ))

            conn.commit()
            execution_id = cursor.lastrowid

            print(f"Logged pipeline execution: {action} - {status}")

            return {
                'id': execution_id,
                'action': action,
                'pipeline_type': action.replace('_', ' ').title(),
                'start_time': datetime.fromtimestamp(start_time),
                'end_time': end_time_val,
                'duration_seconds': duration_val,
                'success': success_val,
                'status': status,
                'result': result[:1000] if result else None
            }

    except Exception as e:
        print(f"Error logging pipeline execution: {e}")
        return None


def update_pipeline_status(execution_id, status, end_time=None, success=None, result=""):
    """Update an existing pipeline execution status."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, skipping status update")
            return False

        with get_target_db_connection() as conn:
            cursor = conn.cursor()

            if status in ['completed', 'failed', 'interrupted']:
                # For final statuses, set end_time and duration
                end_time_val = datetime.fromtimestamp(end_time) if end_time else datetime.now()
                duration_val = None
                if end_time:
                    # Get start time to calculate duration
                    cursor.execute("SELECT start_time FROM chacc_pipeline_history WHERE id = %s", (execution_id,))
                    start_result = cursor.fetchone()
                    if start_result:
                        start_time = start_result[0]
                        if hasattr(start_time, 'timestamp'):
                            duration_val = round(end_time - start_time.timestamp(), 2)

                cursor.execute("""
                    UPDATE chacc_pipeline_history
                    SET status = %s, end_time = %s, duration_seconds = %s, success = %s, result = %s
                    WHERE id = %s
                """, (status, end_time_val, duration_val, success, result[:1000] if result else None, execution_id))
            else:
                # For pending/interrupted status, just update status
                cursor.execute("""
                    UPDATE chacc_pipeline_history
                    SET status = %s, result = %s
                    WHERE id = %s
                """, (status, result[:1000] if result else None, execution_id))

            conn.commit()
            print(f"Updated pipeline execution {execution_id} to status: {status}")
            return True

    except Exception as e:
        print(f"Error updating pipeline status: {e}")
        return False


def get_last_pending_execution():
    """Get the last pending pipeline execution."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning None")
            return None

        with get_target_db_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            cursor.execute("""
                SELECT id, action, pipeline_type, start_time, status, result
                FROM chacc_pipeline_history
                WHERE status IN ('pending', 'running')
                ORDER BY created_at DESC
                LIMIT 1
            """)

            result = cursor.fetchone()
            return result

    except Exception as e:
        print(f"Error retrieving last pending execution: {e}")
        return None


def get_recent_history(limit=50, offset=0, pipeline_type=None, date_from=None, date_to=None, success=None, status=None):
    """Get recent pipeline execution history from database with filtering and pagination."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning empty history")
            return []

        with get_target_db_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            query = """
                SELECT ph.id, ph.action, ph.pipeline_type, ph.start_time, ph.end_time,
                       ph.duration_seconds, ph.success, ph.status, ph.result, ph.created_at,
                       COUNT(pth.id) as total_tasks,
                       SUM(CASE WHEN pth.status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                       SUM(CASE WHEN pth.status = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                       SUM(CASE WHEN pth.status = 'interrupted' THEN 1 ELSE 0 END) as interrupted_tasks
                FROM chacc_pipeline_history ph
                LEFT JOIN pipeline_task_history pth ON ph.id = pth.pipeline_history_id
                WHERE 1=1
            """
            params = []

            if pipeline_type:
                query += " AND ph.action = %s"
                params.append(pipeline_type)

            if date_from:
                query += " AND DATE(ph.start_time) >= %s"
                params.append(date_from)

            if date_to:
                query += " AND DATE(ph.start_time) <= %s"
                params.append(date_to)

            if success is not None:
                query += " AND ph.success = %s"
                params.append(success)

            if status:
                query += " AND ph.status = %s"
                params.append(status)

            query += " GROUP BY ph.id ORDER BY ph.created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])

            cursor.execute(query, params)
            results = cursor.fetchall()

            for result in results:
                result['timestamp'] = result['created_at'].isoformat() if result['created_at'] else None
                result['start_time'] = result['start_time'].isoformat() if result['start_time'] else None
                result['end_time'] = result['end_time'].isoformat() if result['end_time'] else None

            return results

    except Exception as e:
        print(f"Error retrieving pipeline history: {e}")
        return []


def get_pipeline_task_history(pipeline_history_id):
    """Get all tasks that ran in a specific pipeline execution."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning empty task history")
            return []

        with get_target_db_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            cursor.execute("""
                SELECT id, task_name, task_type, status, start_time, end_time,
                       duration_seconds, error_message, records_processed, created_at
                FROM pipeline_task_history
                WHERE pipeline_history_id = %s
                ORDER BY start_time ASC
            """, (pipeline_history_id,))

            results = cursor.fetchall()

            for result in results:
                result['start_time'] = result['start_time'].isoformat() if result['start_time'] else None
                result['end_time'] = result['end_time'].isoformat() if result['end_time'] else None
                result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None

            return results

    except Exception as e:
        print(f"Error retrieving pipeline task history: {e}")
        return []


def get_recent_history(limit=50, offset=0, pipeline_type=None, date_from=None, date_to=None, success=None, status=None):
    """Get recent pipeline execution history from database with filtering and pagination."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning empty history")
            return []

        with get_target_db_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            query = """
                SELECT id, action, pipeline_type, start_time, end_time,
                       duration_seconds, success, status, result, created_at
                FROM chacc_pipeline_history
                WHERE 1=1
            """
            params = []

            if pipeline_type:
                query += " AND action = %s"
                params.append(pipeline_type)

            if date_from:
                query += " AND DATE(start_time) >= %s"
                params.append(date_from)

            if date_to:
                query += " AND DATE(start_time) <= %s"
                params.append(date_to)

            if success is not None:
                query += " AND success = %s"
                params.append(success)

            if status:
                query += " AND status = %s"
                params.append(status)

            query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])

            cursor.execute(query, params)
            results = cursor.fetchall()

            for result in results:
                result['timestamp'] = result['created_at'].isoformat() if result['created_at'] else None
                result['start_time'] = result['start_time'].isoformat() if result['start_time'] else None
                result['end_time'] = result['end_time'].isoformat() if result['end_time'] else None

            return results

    except Exception as e:
        print(f"Error retrieving pipeline history: {e}")
        return []


def get_history_count(pipeline_type=None, date_from=None, date_to=None, success=None, status=None):
    """Get total count of history records with filters."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning 0 count")
            return 0

        with get_target_db_connection() as conn:
            cursor = conn.cursor()

            query = "SELECT COUNT(*) FROM chacc_pipeline_history WHERE 1=1"
            params = []

            if pipeline_type:
                query += " AND action = %s"
                params.append(pipeline_type)

            if date_from:
                query += " AND DATE(start_time) >= %s"
                params.append(date_from)

            if date_to:
                query += " AND DATE(start_time) <= %s"
                params.append(date_to)

            if success is not None:
                query += " AND success = %s"
                params.append(success)

            if status:
                query += " AND status = %s"
                params.append(status)

            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else 0

    except Exception as e:
        print(f"Error counting pipeline history: {e}")
        return 0


def get_pipeline_types():
    """Get list of all available pipeline types for filtering."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning predefined pipeline types")
            return get_predefined_pipeline_types()

        with get_target_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT DISTINCT action, pipeline_type
                FROM chacc_pipeline_history
                ORDER BY action
            """)

            results = cursor.fetchall()

            predefined_types = get_predefined_pipeline_types()
            action_map = {item['action']: item for item in predefined_types}

            for result in results:
                if result[0] not in action_map:
                    action_map[result[0]] = {'action': result[0], 'pipeline_type': result[1]}

            return list(action_map.values())

    except Exception as e:
        print(f"Error retrieving pipeline types: {e}")
        return get_predefined_pipeline_types()


def get_predefined_pipeline_types():
    """Get predefined pipeline types based on available configurations."""
    return [
        {'action': 'full_refresh', 'pipeline_type': 'Full Refresh Pipeline'},
        {'action': 'incremental', 'pipeline_type': 'Incremental Update Pipeline'},
        {'action': 'scheduled_incremental', 'pipeline_type': 'Scheduled Incremental Pipeline'},
        {'action': 'scheduled_full', 'pipeline_type': 'Scheduled Full Refresh Pipeline'},
        {'action': 'force_refresh', 'pipeline_type': 'Force Full Refresh Pipeline'},
        {'action': 'extract_load', 'pipeline_type': 'Extract Load Pipeline'},
        {'action': 'procedure', 'pipeline_type': 'Procedure Pipeline'},
        {'action': 'flattened', 'pipeline_type': 'Flattened Pipeline'},
        {'action': 'schema', 'pipeline_type': 'Schema Pipeline'},
        {'action': 'summary', 'pipeline_type': 'Summary Pipeline'}
    ]


def clear_history():
    """Clear all pipeline execution history from database."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, cannot clear history")
            return False

        with get_target_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE chacc_pipeline_history")
            conn.commit()
            print("Pipeline history cleared successfully")
            return True
    except Exception as e:
        print(f"Error clearing pipeline history: {e}")
        return False


def get_history_stats():
    """Get statistics about pipeline executions from database."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning default stats")
            return {
                'total_runs': 0,
                'success_rate': 0,
                'avg_duration': 0,
                'last_run': None
            }

        with get_target_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_runs,
                    AVG(duration_seconds) as avg_duration,
                    MAX(created_at) as last_run
                FROM chacc_pipeline_history
            """)

            result = cursor.fetchone()

            if not result or result[0] == 0:
                return {
                    'total_runs': 0,
                    'success_rate': 0,
                    'avg_duration': 0,
                    'last_run': None
                }

            total_runs = result[0] or 0
            successful_runs = result[1] or 0
            avg_duration = float(result[2] or 0)
            last_run = result[3]

            success_rate = (successful_runs / total_runs) * 100 if total_runs > 0 else 0

            return {
                'total_runs': total_runs,
                'success_rate': round(success_rate, 1),
                'avg_duration': round(avg_duration, 1),
                'last_run': last_run.isoformat() if last_run else None
            }

    except Exception as e:
        print(f"Error retrieving pipeline stats: {e}")
        return {
            'total_runs': 0,
            'success_rate': 0,
            'avg_duration': 0,
            'last_run': None
        }

