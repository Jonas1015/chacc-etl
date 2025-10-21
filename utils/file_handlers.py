#!/usr/bin/env python3
"""
File Handler Utility Module

Handles file operations for SQL and JSON editors.
"""

import json
import os


def get_file_tree(base_dir, extension):
    """Get file tree structure for a given directory and extension"""
    file_tree = {}

    if os.path.exists(base_dir):
        for root, dirs, files in os.walk(base_dir):
            for file in files:
                if file.endswith(extension):
                    rel_path = os.path.relpath(os.path.join(root, file), base_dir)
                    folder = os.path.dirname(rel_path) if os.path.dirname(rel_path) else 'root'
                    if folder not in file_tree:
                        file_tree[folder] = []
                    file_tree[folder].append(rel_path)

    return file_tree


def handle_file_operation(base_dir, operation, **kwargs):
    """Handle file operations (read, write, delete)"""
    if operation == 'read':
        file_path = kwargs.get('file_path')
        if file_path:
            full_path = os.path.join(base_dir, file_path)
            try:
                with open(full_path, 'r') as f:
                    return f.read()
            except Exception as e:
                return f"Error reading file: {str(e)}"
    elif operation == 'write':
        file_path = kwargs.get('file_path')
        content = kwargs.get('content', '')
        if file_path:
            full_path = os.path.join(base_dir, file_path)
            try:
                with open(full_path, 'w') as f:
                    f.write(content)
                return f"File {file_path} saved successfully!"
            except Exception as e:
                return f"Error saving file: {str(e)}"
    elif operation == 'delete':
        file_path = kwargs.get('file_path')
        if file_path:
            full_path = os.path.join(base_dir, file_path)
            try:
                os.remove(full_path)
                return f"File {file_path} deleted successfully!"
            except Exception as e:
                return f"Error deleting file: {str(e)}"
    elif operation == 'validate_json':
        content = kwargs.get('content', '')
        try:
            json.loads(content)
            return True, "Valid JSON"
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON: {str(e)}"

    return None


def handle_upload(base_dir, uploaded_file, upload_type, custom_filename='', folder_path=''):
    """Handle file upload operations"""
    if custom_filename:
        if upload_type == 'sql' and not custom_filename.endswith('.sql'):
            custom_filename += '.sql'
        elif upload_type == 'json' and not custom_filename.endswith('.json'):
            custom_filename += '.json'
        filename = custom_filename
    else:
        filename = uploaded_file.filename

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
        return "SQL files must have .sql extension"
    elif upload_type == 'json' and not filename.endswith('.json'):
        return "JSON files must have .json extension"
    else:
        try:
            uploaded_file.save(file_path)
            return f"File {filename} uploaded successfully to {folder_path or 'default folder'}!"
        except Exception as e:
            return f"Error uploading file: {str(e)}"