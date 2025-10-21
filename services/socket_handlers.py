# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Socket Handlers Module

Handles WebSocket events for real-time communication.
"""

try:
    from flask_socketio import emit
    HAS_SOCKETIO = True
except ImportError:
    HAS_SOCKETIO = False
    def emit(*args, **kwargs):
        pass

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def handle_connect():
    """Send current status when client connects"""
    if HAS_SOCKETIO:
        from services.progress_service import get_current_progress
        current_progress = get_current_progress()

        if not current_progress.get('running', False):
            from web_ui import task_status
            if HAS_PSUTIL:
                try:
                    import os
                    current_pid = os.getpid()

                    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                        try:
                            if proc.info['name'] in ['python3', 'python', 'python.exe']:
                                cmdline = proc.info['cmdline']
                                if cmdline and len(cmdline) > 1:
                                    cmdline_str = ' '.join(cmdline)
                                    if 'run_pipeline.py' in cmdline_str:
                                        print(f"Detected running pipeline process: {proc.info['pid']} - {cmdline_str}")

                                        pipeline_type = 'Pipeline'
                                        if '--incremental' in cmdline_str:
                                            pipeline_type = 'Incremental Pipeline'
                                        elif '--full-refresh' in cmdline_str:
                                            pipeline_type = 'Full Refresh Pipeline'
                                        elif '--scheduled' in cmdline_str:
                                            pipeline_type = 'Scheduled Pipeline'

                                        current_progress.update({
                                            'running': True,
                                            'message': f'{pipeline_type} is currently running...',
                                            'progress': 50,
                                            'current_task': pipeline_type
                                        })
                                        break
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            continue
                except Exception as e:
                    print(f"Error checking for running pipelines: {e}")

        emit('task_update', current_progress)


def handle_get_status():
    """Handle status requests"""
    if HAS_SOCKETIO:
        from services.progress_service import get_current_progress
        current_progress = get_current_progress()

        if not current_progress.get('running', False):
            from web_ui import task_status
            if HAS_PSUTIL:
                try:
                    import os
                    current_pid = os.getpid()

                    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                        try:
                            if proc.info['name'] in ['python3', 'python', 'python.exe']:
                                cmdline = proc.info['cmdline']
                                if cmdline and len(cmdline) > 1:
                                    cmdline_str = ' '.join(cmdline)
                                    if 'run_pipeline.py' in cmdline_str and proc.info['pid'] != current_pid:
                                        print(f"Status check: Detected running pipeline process: {proc.info['pid']} - {cmdline_str}")

                                        pipeline_type = 'Pipeline'
                                        if '--incremental' in cmdline_str:
                                            pipeline_type = 'Incremental Pipeline'
                                        elif '--full-refresh' in cmdline_str:
                                            pipeline_type = 'Full Refresh Pipeline'
                                        elif '--scheduled' in cmdline_str:
                                            pipeline_type = 'Scheduled Pipeline'

                                        current_progress.update({
                                            'running': True,
                                            'message': f'{pipeline_type} is currently running...',
                                            'progress': 50,
                                            'current_task': pipeline_type
                                        })
                                        break
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            continue
                except Exception as e:
                    print(f"Error checking for running pipelines in status: {e}")

        emit('task_update', current_progress)


def register_socket_handlers(socketio):
    """Register all socket event handlers"""
    if HAS_SOCKETIO and socketio:
        socketio.on_event('connect', handle_connect)
        socketio.on_event('get_status', handle_get_status)