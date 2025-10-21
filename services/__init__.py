# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Services Package

This package contains all the service modules for the ETL pipeline web UI.
"""

from .daemon_service import ensure_luigi_daemon_running, is_daemon_running, stop_daemon
from .pipeline_service import execute_pipeline, get_pipeline_result, get_optimal_worker_count, build_pipeline_command
from .progress_service import estimate_progress, monitor_pipeline_progress, initialize_task_status
from .socket_handlers import register_socket_handlers
from .history_service import log_pipeline_execution, get_recent_history, get_history_stats, clear_history, get_history_count, get_pipeline_types

__all__ = [
    'ensure_luigi_daemon_running',
    'is_daemon_running',
    'stop_daemon',
    'execute_pipeline',
    'get_pipeline_result',
    'get_optimal_worker_count',
    'build_pipeline_command',
    'estimate_progress',
    'monitor_pipeline_progress',
    'initialize_task_status',
    'register_socket_handlers',
    'log_pipeline_execution',
    'get_recent_history',
    'get_history_stats',
    'clear_history'
]