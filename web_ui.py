# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)


"""
Web UI for Source Database Migration Pipeline

Provides a simple web interface to run the ETL pipeline with buttons.
"""

import subprocess
import sys
import os
import json
from flask import Flask, render_template, render_template_string, request, redirect, url_for, send_from_directory

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    status = None

    if request.method == 'POST':
        action = request.form.get('action')

        try:
            cmd = [sys.executable, 'scripts/run_pipeline.py']

            if action == 'full_refresh':
                cmd.append('--full-refresh')
            elif action == 'incremental':
                cmd.append('--incremental')
            elif action == 'scheduled':
                cmd.append('--scheduled')
            elif action == 'force_refresh':
                cmd.append('--full-refresh')
                cmd.append('--force')

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))

            if result.returncode == 0:
                status = f"Pipeline completed successfully!\n\nOutput:\n{result.stdout}"
            else:
                status = f"Pipeline failed with exit code {result.returncode}\n\nError:\n{result.stderr}\n\nOutput:\n{result.stdout}"

        except Exception as e:
            status = f"Error running pipeline: {str(e)}"

    return render_template("index.html", status=status)

@app.route('/sql-editor', methods=['GET', 'POST'])
def sql_editor():
    sql_files = []
    selected_file = request.args.get('file', '')
    content = ''
    message = ''

    # Handle file deletion
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

    # Handle file deletion
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
            # Use custom filename if provided, otherwise use original
            if custom_filename:
                if upload_type == 'sql' and not custom_filename.endswith('.sql'):
                    custom_filename += '.sql'
                elif upload_type == 'json' and not custom_filename.endswith('.json'):
                    custom_filename += '.json'
                filename = custom_filename
            else:
                filename = uploaded_file.filename

            # Determine base directory
            if upload_type == 'sql':
                base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')
            elif upload_type == 'json':
                base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
            else:
                message = "Invalid upload type"
                return render_template("upload_template.html", message=message)

            # Handle folder path
            if folder_path:
                # Create folder if it doesn't exist
                full_dir = os.path.join(base_dir, folder_path)
                os.makedirs(full_dir, exist_ok=True)
            else:
                # Use default folders
                if upload_type == 'sql':
                    full_dir = os.path.join(base_dir, 'extract')
                else:  # json
                    full_dir = os.path.join(base_dir, 'tasks')

            file_path = os.path.join(full_dir, filename)

            # Validate file extension
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
    app.run(debug=True, host='0.0.0.0', port=5000)