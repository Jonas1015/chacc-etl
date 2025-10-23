# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Web UI for Source Database Migration Pipeline

Provides a simple web interface to run the ETL pipeline with buttons.
"""

import os
import sys
import time
from flask import Flask, render_template, request
from flask_socketio import SocketIO

from services.progress_service import initialize_task_status
from services.socket_handlers import register_socket_handlers
from services.history_service import get_recent_history, get_history_stats, clear_history, get_history_count, get_pipeline_types
from utils.pipeline_runner import run_pipeline_async
from utils.file_handlers import get_file_tree, handle_file_operation, handle_upload
from utils.progress_parser import get_current_progress
from services.history_service import get_last_pending_execution, update_pipeline_status, log_pipeline_execution

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)

redis_url = os.getenv('REDIS_URL')
if redis_url:
    socketio = SocketIO(app, cors_allowed_origins="*", client_manager=SocketIO.RedisManager(redis_url))
else:
    socketio = SocketIO(app, cors_allowed_origins="*")

task_status = initialize_task_status()



@app.route('/', methods=['GET', 'POST'])
def index():
    current_progress = get_current_progress()
    last_pendings = None
    if current_progress.get('running', False):
        last_pendings = get_last_pending_execution()
        if len(last_pendings) == 0:
                pipeline_start_time = current_progress.get('pipeline_start_time', time.time())
                pipeline_type = current_progress.get('pipeline_type', 'unknown')
                log_pipeline_execution(pipeline_type, pipeline_start_time, None, None, "Pipeline started", "pending")
        return render_template("index.html", status="A pipeline is already running. Please wait for it to complete.")

    if request.method == 'POST':
        action = request.form.get('action')

        if current_progress.get('running', False) and action != 'force_refresh':
            last_pendings = get_last_pending_execution() if last_pendings == None else last_pendings
            if len(last_pendings) == 0:
                for last_pending in last_pendings:
                    update_pipeline_status(last_pending['id'], 'interrupted', result="Interrupted by new pipeline execution")
        elif action == 'force_refresh' and current_progress.get('running', False):
            try:
                from utils.db_utils import get_target_db_connection
                with get_target_db_connection() as conn:
                    cursor = conn.cursor()
                    import json
                    interruption_data = {
                        "interruption_requested": True,
                        "requested_by": "web_ui",
                        "reason": "Force refresh requested",
                        "requested_at": time.time()
                    }
                    cursor.execute("""
                        UPDATE chacc_pipeline_history
                        SET status = 'interrupting',
                            result = %s
                        WHERE status IN ('pending', 'running')
                    """, (json.dumps(interruption_data),))
                    conn.commit()
                    print("Set interruption flag for running pipelines")
            except Exception as e:
                print(f"Failed to set interruption flag: {e}")

        pipeline_start_time = current_progress.get('pipeline_start_time', time.time())
        pipeline_type = current_progress.get('pipeline_type', action)
        log_pipeline_execution(pipeline_type, pipeline_start_time, None, None, "Pipeline started", "pending")

        import threading
        thread = threading.Thread(target=run_pipeline_async, args=(action, socketio, task_status))
        thread.daemon = True
        thread.start()

        return render_template("index.html", status="Pipeline started in background. Check progress below.")

    if last_pendings is None:
        last_pendings = get_last_pending_execution()
    
    if len(last_pendings) > 0:
            for last_pending in last_pendings:
                update_pipeline_status(execution_id = last_pending.get("id"), status = "interrupted", end_time = None, success = False, result = "Pipeline started")

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
    status = None
    if status_filter == 'success':
        success = True
    elif status_filter == 'failed':
        success = False
    elif status_filter in ['pending', 'running', 'interrupted', 'completed']:
        status = status_filter

    offset = (page - 1) * per_page

    history_data = get_recent_history(
        limit=per_page,
        offset=offset,
        pipeline_type=pipeline_type if pipeline_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        success=success,
        status=status
    )

    total_count = get_history_count(
        pipeline_type=pipeline_type if pipeline_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        success=success,
        status=status
    )

    pipeline_types = get_pipeline_types()

    stats = get_history_stats()

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

@app.route('/pipeline-tasks/<int:pipeline_id>')
def pipeline_tasks(pipeline_id):
    """Get tasks for a specific pipeline execution."""
    from services.history_service import get_pipeline_task_history
    tasks = get_pipeline_task_history(pipeline_id)
    return {'tasks': tasks}

@app.route('/clear-history', methods=['POST'])
def clear_history_route():
    """Clear all pipeline execution history."""
    try:
        clear_history()
        return {'success': True, 'message': 'History cleared successfully'}
    except Exception as e:
        return {'success': False, 'message': str(e)}, 500

register_socket_handlers(socketio)

@app.route('/sql-editor', methods=['GET', 'POST'])
def sql_editor():
    selected_file = request.args.get('file', '')
    content = ''
    message = ''

    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')

    if request.method == 'POST':
        if 'delete' in request.form:
            message = handle_file_operation(sql_dir, 'delete', file_path=request.form.get('delete'))
            if selected_file == request.form.get('delete'):
                selected_file = ''
        elif 'content' in request.form and selected_file:
            content = request.form.get('content', '')
            message = handle_file_operation(sql_dir, 'write', file_path=selected_file, content=content)

    file_tree = get_file_tree(sql_dir, '.sql')

    if selected_file and not request.method == 'POST':
        content = handle_file_operation(sql_dir, 'read', file_path=selected_file)
        if isinstance(content, str) and content.startswith('Error'):
            message = content
            content = ''

    return render_template("sql_editor.html", file_tree=file_tree, selected_file=selected_file, content=content, message=message)

@app.route('/json-editor', methods=['GET', 'POST'])
def json_editor():
    selected_file = request.args.get('file', '')
    content = ''
    message = ''

    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')

    if request.method == 'POST':
        if 'delete' in request.form:
            message = handle_file_operation(config_dir, 'delete', file_path=request.form.get('delete'))
            if selected_file == request.form.get('delete'):
                selected_file = ''
        elif 'content' in request.form and selected_file:
            content = request.form.get('content', '')
            is_valid, validation_msg = handle_file_operation(config_dir, 'validate_json', content=content)
            if is_valid:
                message = handle_file_operation(config_dir, 'write', file_path=selected_file, content=content)
            else:
                message = validation_msg

    file_tree = get_file_tree(config_dir, '.json')

    if selected_file and not request.method == 'POST':
        content = handle_file_operation(config_dir, 'read', file_path=selected_file)
        if isinstance(content, str) and content.startswith('Error'):
            message = content
            content = ''

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
            base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  'sql' if upload_type == 'sql' else 'config')
            message = handle_upload(base_dir, uploaded_file, upload_type, custom_filename, folder_path)
        else:
            message = "No file selected or invalid upload type"

    return render_template("upload_template.html", message=message)

def initialize_database():
    """Initialize database and required tables on app startup."""
    try:
        print("Initializing database and required tables...")

        from tasks.dynamic_task_factory import create_dynamic_tasks
        from utils.progress_parser import initialize_progress_tracking
        from services.progress_service import initialize_task_status

        dynamic_tasks = create_dynamic_tasks()

        required_tasks = [
            'InitCreateDatabaseTask',
            'TablesEtlMetadataTask',
            'TablesEtlWatermarksTask',
            'TablesPipelineHistoryTask',
            'TablesPipelineTaskHistoryTask'
        ]

        for task_name in required_tasks:
            if task_name in dynamic_tasks:
                task_class = dynamic_tasks[task_name]
                print(f"Running {task_name}...")
                try:
                    task_instance = task_class()
                    task_instance.run()
                    print(f"✓ {task_name} completed successfully")
                except Exception as e:
                    print(f"✗ {task_name} failed: {e}")
            else:
                print(f"Warning: {task_name} not found in task definitions")

        print("Database initialization completed")

    except Exception as e:
        print(f"Database initialization failed: {e}")

if __name__ == '__main__':

    print("Starting Database ETL Pipeline Web UI...")
    print("Open http://localhost:5000 in your browser")
    print("For task visualization, start Luigi daemon: luigid --background --logdir logs/")
    initialize_database()
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)