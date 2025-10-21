# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Web UI for Source Database Migration Pipeline

Provides a simple web interface to run the ETL pipeline with buttons.
"""

import os
import sys
from flask import Flask, render_template, request
from flask_socketio import SocketIO

from services.progress_service import initialize_task_status
from services.socket_handlers import register_socket_handlers
from services.history_service import get_recent_history, get_history_stats, clear_history, get_history_count, get_pipeline_types
from utils.pipeline_runner import run_pipeline_async
from utils.file_handlers import get_file_tree, handle_file_operation, handle_upload

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

task_status = initialize_task_status()


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        action = request.form.get('action')

        if task_status['running']:
            return render_template("index.html", status="A pipeline is already running. Please wait for it to complete.")

        import threading
        thread = threading.Thread(target=run_pipeline_async, args=(action, socketio, task_status))
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

    total_count = get_history_count(
        pipeline_type=pipeline_type if pipeline_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        success=success
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

if __name__ == '__main__':
    print("Starting Database ETL Pipeline Web UI...")
    print("Open http://localhost:5000 in your browser")
    print("For task visualization, start Luigi daemon: luigid --background --logdir logs/")
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)