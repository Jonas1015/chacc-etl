#!/usr/bin/env python3
"""
Pipeline Runner Utility Module

Handles asynchronous pipeline execution with progress monitoring.
"""

import fcntl
import os
import threading
import time
from utils.progress_parser import parse_luigi_progress


def run_pipeline_async(action, socketio, task_status):
    """Run pipeline in background thread with progress updates"""
    start_time = time.time()

    try:
        task_status.update({
            'running': True,
            'current_task': action,
            'progress': 0,
            'message': f"Starting {action.replace('_', ' ')} pipeline...",
            'start_time': start_time
        })
        socketio.emit('task_update', task_status)

        # Send initial progress update to show the UI
        initial_progress = {
            'running': True,
            'progress': 5,
            'message': f"Initializing {action.replace('_', ' ')} pipeline...",
            'current_task': action.replace('_', ' ').title()
        }
        socketio.emit('task_update', initial_progress)

        from services.daemon_service import ensure_luigi_daemon_running
        ensure_luigi_daemon_running(socketio)

        if action == 'scheduled_incremental':
            task_name = "Scheduled Incremental Pipeline"
        elif action == 'scheduled_full':
            task_name = "Scheduled Full Refresh Pipeline"
        else:
            task_name = action.replace('_', ' ').title() + " Pipeline"

        from services.progress_service import initialize_progress_tracking
        initialize_progress_tracking(socketio, action)

        from services.pipeline_service import execute_pipeline
        process = execute_pipeline(action, socketio)

        def monitor_output():
            """Monitor stdout and stderr for progress information"""
            last_update_time = time.time()

            while process.poll() is None:
                current_time = time.time()

                if process.stdout:
                    try:
                        fd = process.stdout.fileno()
                        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

                        try:
                            line = process.stdout.readline()
                            if line:
                                line_str = line.decode('utf-8', errors='ignore').strip()
                                if line_str:
                                    progress_info = parse_luigi_progress(line_str, action)
                                    if progress_info:
                                        socketio.emit('task_update', progress_info)
                                        last_update_time = current_time
                        except (OSError, IOError):
                            pass
                    except (AttributeError, ImportError):
                        pass

                if process.stderr:
                    try:
                        fd = process.stderr.fileno()
                        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

                        try:
                            line = process.stderr.readline()
                            if line:
                                line_str = line.decode('utf-8', errors='ignore').strip()
                                if line_str:
                                    progress_info = parse_luigi_progress(line_str, action)
                                    if progress_info:
                                        socketio.emit('task_update', progress_info)
                                        last_update_time = current_time
                        except (OSError, IOError):
                            pass
                    except (AttributeError, ImportError):
                        pass

                # Send periodic heartbeat updates if no progress for 3 seconds
                if current_time - last_update_time > 3:
                    elapsed = current_time - start_time
                    heartbeat_progress = {
                        'running': True,
                        'progress': min(90, 10 + int(elapsed / 10)),  # Slow progress indication
                        'message': f"Pipeline running... ({int(elapsed)}s elapsed)",
                        'current_task': action.replace('_', ' ').title()
                    }
                    socketio.emit('task_update', heartbeat_progress)
                    last_update_time = current_time

                time.sleep(0.1)

        monitor_thread = threading.Thread(target=monitor_output, daemon=True)
        monitor_thread.start()

        process.wait()

        from services.progress_service import finalize_progress_tracking
        success = process.returncode == 0
        finalize_progress_tracking(success)

        from services.pipeline_service import get_pipeline_result
        result = get_pipeline_result(process, task_name)
        end_time = time.time()

        success = "successfully" in result.get('message', '').lower()
        from services.history_service import log_pipeline_execution
        log_pipeline_execution(action, start_time, end_time, success, result.get('result', ''))

        final_result = {
            **result,
            'running': False,
            'current_task': None,
            'progress': 100
        }
        socketio.emit('task_update', final_result)

    except Exception as e:
        end_time = time.time()
        error_msg = f"Error running pipeline: {str(e)}"

        task_status.update({
            'progress': 100,
            'message': f"Error: {str(e)}"
        })

        from services.history_service import log_pipeline_execution
        log_pipeline_execution(action, start_time, end_time, False, error_msg)

        socketio.emit('task_update', {
            **task_status,
            'result': error_msg
        })

    finally:
        task_status.update({
            'running': False,
            'current_task': None
        })