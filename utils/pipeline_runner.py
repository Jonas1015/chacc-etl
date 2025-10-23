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
from utils.task_manager import LuigiTaskManager


_task_manager = None

def get_task_manager():
    """Get or create the global task manager instance."""
    global _task_manager
    if _task_manager is None:
        _task_manager = LuigiTaskManager(
            max_concurrent_tasks=2,
            task_timeout=1800
        )
    return _task_manager

def run_pipeline_async(action, socketio, task_status):
    """Run pipeline in background thread with progress updates"""
    start_time = time.time()

    if action == 'force_refresh':
        try:
            import psutil
            import signal
            current_process = psutil.Process()
            parent = current_process.parent()

            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'python' in proc.info['name'].lower():
                        cmdline = proc.info.get('cmdline', [])
                        if cmdline and 'run_pipeline.py' in ' '.join(cmdline):
                            print(f"Terminating existing pipeline process: {proc.info['pid']}")
                            proc.terminate()
                            try:
                                proc.wait(timeout=5)
                            except psutil.TimeoutExpired:
                                proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except ImportError:
            print("psutil not available, cannot terminate existing processes")
        except Exception as e:
            print(f"Error terminating existing processes: {e}")

    try:
        task_status.update({
            'running': True,
            'current_task': action,
            'progress': 0,
            'message': f"Starting {action.replace('_', ' ')} pipeline...",
            'start_time': start_time
        })
        socketio.emit('task_update', task_status)

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

        from utils.progress_parser import initialize_progress_tracking as init_parser_progress
        init_parser_progress(action)

        if action == 'force_refresh':
            time.sleep(2)

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

                if current_time - last_update_time > 3:
                    elapsed = current_time - start_time
                    heartbeat_progress = {
                        'running': True,
                        # 'progress': min(90, 10 + int(elapsed / 10)),
                        'message': f"Pipeline running... ({int(elapsed)}s elapsed)",
                        'current_task': action.replace('_', ' ').title()
                    }
                    socketio.emit('task_update', heartbeat_progress)
                    last_update_time = current_time

                time.sleep(0.1)

        monitor_thread = threading.Thread(target=monitor_output, daemon=True)
        monitor_thread.start()

        process.wait()

        
        from services.pipeline_service import get_pipeline_result
        result = get_pipeline_result(process, task_name)
        end_time = time.time()

        success = "successfully" in result.get('message', '').lower()
        
        from services.history_service import log_pipeline_execution
        print("""Logging pipeline execution to history...""", result.get('result', ''))
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