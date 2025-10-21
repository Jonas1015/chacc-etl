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


def log_pipeline_execution(action, start_time, end_time, success, result=""):
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
                CREATE TABLE IF NOT EXISTS pipeline_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    action VARCHAR(50) NOT NULL,
                    pipeline_type VARCHAR(100) NOT NULL,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME NOT NULL,
                    duration_seconds DECIMAL(10,2) NOT NULL,
                    success BOOLEAN NOT NULL DEFAULT FALSE,
                    result TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_action (action),
                    INDEX idx_success (success),
                    INDEX idx_start_time (start_time),
                    INDEX idx_pipeline_type (pipeline_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                INSERT INTO pipeline_history
                (action, pipeline_type, start_time, end_time, duration_seconds, success, result)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                action,
                action.replace('_', ' ').title(),
                datetime.fromtimestamp(start_time),
                datetime.fromtimestamp(end_time),
                round(end_time - start_time, 2),
                success,
                result[:1000] if result else None
            ))

            conn.commit()
            execution_id = cursor.lastrowid

            print(f"Logged pipeline execution: {action} - {'Success' if success else 'Failed'}")

            return {
                'id': execution_id,
                'action': action,
                'pipeline_type': action.replace('_', ' ').title(),
                'start_time': datetime.fromtimestamp(start_time),
                'end_time': datetime.fromtimestamp(end_time),
                'duration_seconds': round(end_time - start_time, 2),
                'success': success,
                'result': result[:1000] if result else None
            }

    except Exception as e:
        print(f"Error logging pipeline execution: {e}")
        return None


def get_recent_history(limit=50, offset=0, pipeline_type=None, date_from=None, date_to=None, success=None):
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
                       duration_seconds, success, result, created_at
                FROM pipeline_history
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


def get_history_count(pipeline_type=None, date_from=None, date_to=None, success=None):
    """Get total count of history records with filters."""
    try:
        try:
            from utils.db_utils import get_target_db_connection
        except ImportError:
            print("Database utils not available, returning 0 count")
            return 0

        with get_target_db_connection() as conn:
            cursor = conn.cursor()

            query = "SELECT COUNT(*) FROM pipeline_history WHERE 1=1"
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
                FROM pipeline_history
                ORDER BY action
            """)

            results = cursor.fetchall()

            # Always include predefined types, plus any from history
            predefined_types = get_predefined_pipeline_types()
            action_map = {item['action']: item for item in predefined_types}

            # Add any types from history that aren't in predefined
            for result in results:
                if result['action'] not in action_map:
                    action_map[result['action']] = result

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
            cursor.execute("TRUNCATE TABLE pipeline_history")
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
                FROM pipeline_history
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

