# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Web UI for Source Database Migration Pipeline

Provides a simple web interface to run the ETL pipeline with buttons.
"""

import fcntl
import os
import threading
import time
from flask import Flask, render_template, request, send_from_directory
from flask_socketio import SocketIO

from services.daemon_service import ensure_luigi_daemon_running
from services.pipeline_service import execute_pipeline, get_pipeline_result
from services.progress_service import monitor_pipeline_progress, initialize_task_status
from services.socket_handlers import register_socket_handlers
from services.history_service import log_pipeline_execution, get_recent_history, get_history_stats, clear_history, get_history_count, get_pipeline_types

import select
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
def parse_luigi_progress(line, action):
    """Parse progress information from Luigi output"""
    import re

    patterns = [
        r'(\d+)% complete',
        r'progress: (\d+)%',
        r'(\d+)/(\d+) tasks?',
        r'Running (\w+)',
        r'Completed (\w+)',
        r'Scheduled (\w+)',
        r'INFO.*luigi.*: (.*)',
    ]

    for pattern in patterns:
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            if '%' in pattern:
                try:
                    progress = int(match.group(1))
                    return {
                        'running': True,
                        'progress': min(progress, 95),
                        'message': f"Pipeline running... {progress}% complete",
                        'current_task': action.replace('_', ' ').title()
                    }
                except (ValueError, IndexError):
                    continue
            elif 'tasks' in pattern.lower():
                try:
                    completed = int(match.group(1))
                    total = int(match.group(2))
                    progress = min(int((completed / total) * 100), 95)
                    return {
                        'running': True,
                        'progress': progress,
                        'message': f"Running pipeline... ({completed}/{total} tasks completed)",
                        'current_task': action.replace('_', ' ').title()
                    }
                except (ValueError, IndexError, ZeroDivisionError):
                    continue
            elif 'Running' in pattern:
                task_name = match.group(1)
                return {
                    'running': True,
                    'message': f"Running {task_name}...",
                    'current_task': task_name
                }
            elif 'Completed' in pattern:
                task_name = match.group(1)
                return {
                    'running': True,
                    'message': f"Completed {task_name}",
                    'current_task': task_name
                }
            elif 'Scheduled' in pattern:
                task_name = match.group(1)
                return {
                    'running': True,
                    'message': f"Scheduled {task_name}",
                    'current_task': task_name
                }
            elif 'INFO' in pattern:
                info_msg = match.group(1)
                return {
                    'running': True,
                    'message': info_msg,
                    'current_task': action.replace('_', ' ').title()
                }

    if any(keyword in line.lower() for keyword in ['starting', 'initializing', 'connecting', 'processing']):
        return {
            'running': True,
            'message': line.strip(),
            'current_task': action.replace('_', ' ').title()
        }

    return None

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

task_status = initialize_task_status()

def run_pipeline_async(action):
    """Run pipeline in background thread with progress updates"""
    global task_status

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

        ensure_luigi_daemon_running(socketio)

        if action == 'scheduled_incremental':
            task_name = "Scheduled Incremental Pipeline"
        elif action == 'scheduled_full':
            task_name = "Scheduled Full Refresh Pipeline"
        else:
            task_name = action.replace('_', ' ').title() + " Pipeline"

        from services.progress_service import initialize_progress_tracking
        initialize_progress_tracking(socketio, action)

        process = execute_pipeline(action, socketio)

        # Monitor process output in real-time for progress updates
        import threading

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

        result = get_pipeline_result(process, task_name)
        end_time = time.time()

        success = "successfully" in result.get('message', '').lower()
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

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        action = request.form.get('action')

        if task_status['running']:
            return render_template("index.html", status="A pipeline is already running. Please wait for it to complete.")

        thread = threading.Thread(target=run_pipeline_async, args=(action,))
        thread.daemon = True
        thread.start()

        return render_template("index.html", status="Pipeline started in background. Check progress below.")

    return render_template("index.html", status=None)

@app.route('/history')
def history():
    """Display pipeline execution history with filtering and pagination."""
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 25))
    pipeline_type = request.args.get('pipeline_type', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    status_filter = request.args.get('status', '')

    success = None
    if status_filter == 'success':
        success = True
    elif status_filter == 'failed':
        success = False

    offset = (page - 1) * per_page

    history_data = get_recent_history(
        limit=per_page,
        offset=offset,
        pipeline_type=pipeline_type if pipeline_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        success=success
    )

    # Get total count for pagination
    total_count = get_history_count(
        pipeline_type=pipeline_type if pipeline_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        success=success
    )

    # Get pipeline types for filter dropdown
    pipeline_types = get_pipeline_types()

    # Get stats (these are global, not filtered)
    stats = get_history_stats()

    # Calculate pagination info
    total_pages = (total_count + per_page - 1) // per_page

    return render_template('history.html',
                         history=history_data,
                         stats=stats,
                         pipeline_types=pipeline_types,
                         page=page,
                         per_page=per_page,
                         total_pages=total_pages,
                         total_count=total_count,
                         filters={
                             'pipeline_type': pipeline_type,
                             'date_from': date_from,
                             'date_to': date_to,
                             'status': status_filter
                         })

@app.route('/clear-history', methods=['POST'])
def clear_history_route():
    """Clear all pipeline execution history."""
    try:
        clear_history()
        return {'success': True, 'message': 'History cleared successfully'}
    except Exception as e:
        return {'success': False, 'message': str(e)}, 500

# Register WebSocket event handlers
register_socket_handlers(socketio)

@app.route('/sql-editor', methods=['GET', 'POST'])
def sql_editor():
    sql_files = []
    selected_file = request.args.get('file', '')
    content = ''
    message = ''

    if request.method == 'POST' and 'delete' in request.form:
        delete_file = request.form.get('delete')
        sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')
        file_path = os.path.join(sql_dir, delete_file)
        try:
            os.remove(file_path)
            message = f"File {delete_file} deleted successfully!"
            if selected_file == delete_file:
                selected_file = ''
        except Exception as e:
            message = f"Error deleting file: {str(e)}"

    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')
    file_tree = {}

    if os.path.exists(sql_dir):
        for root, dirs, files in os.walk(sql_dir):
            for file in files:
                if file.endswith('.sql'):
                    rel_path = os.path.relpath(os.path.join(root, file), sql_dir)
                    folder = os.path.dirname(rel_path) if os.path.dirname(rel_path) else 'root'
                    if folder not in file_tree:
                        file_tree[folder] = []
                    file_tree[folder].append(rel_path)

    if selected_file and request.method == 'POST' and 'content' in request.form:
        content = request.form.get('content', '')
        file_path = os.path.join(sql_dir, selected_file)
        try:
            with open(file_path, 'w') as f:
                f.write(content)
            message = f"File {selected_file} saved successfully!"
        except Exception as e:
            message = f"Error saving file: {str(e)}"
    elif selected_file:
        file_path = os.path.join(sql_dir, selected_file)
        try:
            with open(file_path, 'r') as f:
                content = f.read()
        except Exception as e:
            message = f"Error reading file: {str(e)}"

    return render_template("sql_editor.html", file_tree=file_tree, selected_file=selected_file, content=content, message=message)

@app.route('/json-editor', methods=['GET', 'POST'])
def json_editor():
    json_files = []
    selected_file = request.args.get('file', '')
    content = ''
    message = ''

    if request.method == 'POST' and 'delete' in request.form:
        delete_file = request.form.get('delete')
        config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
        file_path = os.path.join(config_dir, delete_file)
        try:
            os.remove(file_path)
            message = f"File {delete_file} deleted successfully!"
            if selected_file == delete_file:
                selected_file = ''
        except Exception as e:
            message = f"Error deleting file: {str(e)}"

    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
    file_tree = {}

    if os.path.exists(config_dir):
        for root, dirs, files in os.walk(config_dir):
            for file in files:
                if file.endswith('.json'):
                    rel_path = os.path.relpath(os.path.join(root, file), config_dir)
                    folder = os.path.dirname(rel_path) if os.path.dirname(rel_path) else 'root'
                    if folder not in file_tree:
                        file_tree[folder] = []
                    file_tree[folder].append(rel_path)

    if selected_file and request.method == 'POST' and 'content' in request.form:
        content = request.form.get('content', '')
        file_path = os.path.join(config_dir, selected_file)
        try:
            json.loads(content)
            with open(file_path, 'w') as f:
                f.write(content)
            message = f"File {selected_file} saved successfully!"
        except json.JSONDecodeError as e:
            message = f"Invalid JSON: {str(e)}"
        except Exception as e:
            message = f"Error saving file: {str(e)}"
    elif selected_file:
        file_path = os.path.join(config_dir, selected_file)
        try:
            with open(file_path, 'r') as f:
                content = f.read()
        except Exception as e:
            message = f"Error reading file: {str(e)}"

    return render_template("json_editor.html", file_tree=file_tree, selected_file=selected_file, content=content, message=message)

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    message = ''
    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        upload_type = request.form.get('type')
        custom_filename = request.form.get('filename', '').strip()
        folder_path = request.form.get('folder', '').strip()

        if uploaded_file and upload_type:
            if custom_filename:
                if upload_type == 'sql' and not custom_filename.endswith('.sql'):
                    custom_filename += '.sql'
                elif upload_type == 'json' and not custom_filename.endswith('.json'):
                    custom_filename += '.json'
                filename = custom_filename
            else:
                filename = uploaded_file.filename

            if upload_type == 'sql':
                base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')
            elif upload_type == 'json':
                base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
            else:
                message = "Invalid upload type"
                return render_template("upload_template.html", message=message)

            if folder_path:
                full_dir = os.path.join(base_dir, folder_path)
                os.makedirs(full_dir, exist_ok=True)
            else:
                if upload_type == 'sql':
                    full_dir = os.path.join(base_dir, 'extract')
                else:
                    full_dir = os.path.join(base_dir, 'tasks')

            file_path = os.path.join(full_dir, filename)

            if upload_type == 'sql' and not filename.endswith('.sql'):
                message = "SQL files must have .sql extension"
            elif upload_type == 'json' and not filename.endswith('.json'):
                message = "JSON files must have .json extension"
            else:
                try:
                    uploaded_file.save(file_path)
                    message = f"File {filename} uploaded successfully to {folder_path or 'default folder'}!"
                except Exception as e:
                    message = f"Error uploading file: {str(e)}"
        else:
            message = "No file selected or invalid upload type"

    return render_template("upload_template.html", message=message)

if __name__ == '__main__':
    print("Starting Database ETL Pipeline Web UI...")
    print("Open http://localhost:5000 in your browser")
    print("For task visualization, start Luigi daemon: luigid --background --logdir logs/")
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)