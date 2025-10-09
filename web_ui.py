#!/usr/bin/env python3
"""
Web UI for OpenMRS Database Migration Pipeline

Provides a simple web interface to run the ETL pipeline with buttons.
"""

import subprocess
import sys
import os
import json
from flask import Flask, render_template_string, request, redirect, url_for, send_from_directory

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Database ETL Pipeline</title>
    <style>
        :root {
            --bg-color: #f8f9fa;
            --text-color: #212529;
            --card-bg: #ffffff;
            --border-color: #dee2e6;
            --header-border: #e9ecef;
            --button-bg: #007bff;
            --button-hover: #0056b3;
            --button-secondary: #6c757d;
            --button-secondary-hover: #5a6268;
            --success-bg: #d4edda;
            --success-text: #155724;
            --error-bg: #f8d7da;
            --error-text: #721c24;
            --info-bg: #d1ecf1;
            --info-text: #0c5460;
            --shadow: rgba(0,0,0,0.1);
        }

        [data-theme="dark"] {
            --bg-color: #1a1a1a;
            --text-color: #e9ecef;
            --card-bg: #2d3748;
            --border-color: #4a5568;
            --header-border: #4a5568;
            --button-bg: #3182ce;
            --button-hover: #2c5282;
            --button-secondary: #718096;
            --button-secondary-hover: #4a5568;
            --success-bg: #38a169;
            --success-text: #f0fff4;
            --error-bg: #e53e3e;
            --error-text: #fed7d7;
            --info-bg: #3182ce;
            --info-text: #ebf8ff;
            --shadow: rgba(0,0,0,0.3);
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
            transition: background-color 0.3s, color 0.3s;
        }
        .container {
            max-width: 900px;
            margin: 0 auto;
            background: var(--card-bg);
            border-radius: 8px;
            box-shadow: 0 2px 10px var(--shadow);
            padding: 40px;
            transition: background-color 0.3s;
        }
        .header {
            position: relative;
            text-align: center;
            margin-bottom: 40px;
            border-bottom: 2px solid var(--header-border);
            padding-bottom: 20px;
        }
        .theme-toggle {
            position: absolute;
            top: 0;
            right: 0;
        }
        .theme-btn {
            background: var(--button-secondary);
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            transition: background-color 0.3s;
        }
        .theme-btn:hover {
            background: var(--button-secondary-hover);
        }
        .header h1 {
            color: var(--text-color);
            margin-bottom: 10px;
            font-size: 2.2em;
            font-weight: 300;
        }
        .header p {
            color: #6c757d;
            font-size: 1.1em;
            margin: 0;
        }
        [data-theme="dark"] .header p {
            color: #adb5bd;
        }
        .button-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }
        .button {
            background-color: var(--button-bg);
            border: 1px solid var(--button-bg);
            color: white;
            padding: 16px 24px;
            text-align: center;
            text-decoration: none;
            display: block;
            font-size: 16px;
            font-weight: 500;
            cursor: pointer;
            border-radius: 6px;
            transition: all 0.2s ease;
            box-shadow: 0 2px 4px rgba(0,123,255,0.2);
        }
        .button:hover {
            background-color: var(--button-hover);
            border-color: var(--button-hover);
            transform: translateY(-1px);
            box-shadow: 0 4px 8px rgba(0,123,255,0.3);
        }
        .button.danger {
            background-color: #dc3545;
            border-color: #dc3545;
            box-shadow: 0 2px 4px rgba(220,53,69,0.2);
        }
        .button.danger:hover {
            background-color: #c82333;
            border-color: #bd2130;
            box-shadow: 0 4px 8px rgba(220,53,69,0.3);
        }
        .button.secondary {
            background-color: var(--button-secondary);
            border-color: var(--button-secondary);
            box-shadow: 0 2px 4px rgba(108,117,125,0.2);
        }
        .button.secondary:hover {
            background-color: var(--button-secondary-hover);
            border-color: var(--button-secondary-hover);
            box-shadow: 0 4px 8px rgba(108,117,125,0.3);
        }
        .status {
            margin-top: 30px;
            padding: 24px;
            border-radius: 6px;
            border-left: 4px solid;
            background-color: var(--card-bg);
            color: var(--text-color);
        }
        .success {
            border-color: var(--success-bg);
            background-color: var(--success-bg);
            color: var(--success-text);
        }
        .error {
            border-color: #dc3545;
            background-color: var(--error-bg);
            color: var(--error-text);
        }
        .info {
            border-color: var(--info-bg);
            background-color: var(--info-bg);
            color: var(--info-text);
        }
        .status h3 {
            margin-top: 0;
            margin-bottom: 12px;
            font-size: 1.2em;
            font-weight: 600;
        }
        .status pre {
            white-space: pre-wrap;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 14px;
            line-height: 1.4;
            margin: 0;
            background-color: rgba(255,255,255,0.7);
            padding: 12px;
            border-radius: 4px;
            overflow-x: auto;
        }
        .tool-buttons { margin-bottom: 40px; }
        .tool-btn { display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 80px; }
        .tool-btn br { display: none; }
        .footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 24px;
            border-top: 1px solid var(--border-color);
            color: #6c757d;
            font-size: 0.9em;
        }
        [data-theme="dark"] .footer {
            color: #adb5bd;
            border-top-color: var(--border-color);
        }
    </style>
    <script>
        function toggleTheme() {
            const html = document.documentElement;
            const themeBtn = document.getElementById('theme-toggle');

            if (html.getAttribute('data-theme') === 'dark') {
                html.removeAttribute('data-theme');
                themeBtn.textContent = '🌙 Dark Mode';
                localStorage.setItem('theme', 'light');
            } else {
                html.setAttribute('data-theme', 'dark');
                themeBtn.textContent = '☀️ Light Mode';
                localStorage.setItem('theme', 'dark');
            }
        }

        // Load saved theme
        document.addEventListener('DOMContentLoaded', function() {
            const savedTheme = localStorage.getItem('theme');
            const themeBtn = document.getElementById('theme-toggle');

            if (savedTheme === 'dark') {
                document.documentElement.setAttribute('data-theme', 'dark');
                themeBtn.textContent = '☀️ Light Mode';
            } else {
                themeBtn.textContent = '🌙 Dark Mode';
            }
        });
    </script>
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="theme-toggle">
                <button onclick="toggleTheme()" class="theme-btn" id="theme-toggle">🌙 Dark Mode</button>
            </div>
            <h1>Database ETL Pipeline</h1>
            <p>Enterprise-grade data migration and analytics pipeline built with Luigi</p>
        </div>

        <div class="tool-buttons" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 40px;">
            <a href="/sql-editor" class="button tool-btn" style="text-decoration: none; text-align: center;">
                SQL Editor
            </a>
            <a href="/json-editor" class="button tool-btn" style="text-decoration: none; text-align: center;">
                JSON Editor
            </a>
            <a href="/upload" class="button secondary tool-btn" style="text-decoration: none; text-align: center;">
                Upload Files
            </a>
            <a href="http://localhost:8082" target="_blank" class="button secondary tool-btn" style="text-decoration: none; text-align: center;">
                Luigi Visualizer
            </a>
        </div>

        <form method="post">
            <div class="button-grid">
                <button class="button" name="action" value="full_refresh" type="submit">Full Refresh</button>
                <button class="button" name="action" value="incremental" type="submit">Incremental Update</button>
                <button class="button" name="action" value="scheduled" type="submit">Scheduled Run</button>
                <button class="button danger" name="action" value="force_refresh" type="submit">Force Full Refresh</button>
                <a href="http://localhost:8082" target="_blank" class="button secondary" style="text-decoration: none; text-align: center; line-height: 1.2;">Luigi Visualizer</a>
            </div>
        </form>

        {% if status %}
        <div class="status {{ 'success' if 'successfully' in status.lower() else 'error' if 'error' in status.lower() or 'failed' in status.lower() else 'info' }}">
            <h3>Execution Status</h3>
            <pre>{{ status }}</pre>
        </div>
        {% endif %}

        <div class="footer">
            <p>Built with Luigi and Flask</p>
        </div>
    </div>
</body>
</html>
"""

SQL_EDITOR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>SQL Editor - Database ETL Pipeline</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; margin: 0; padding: 20px; background-color: #f8f9fa; }
        .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 30px; }
        .header { border-bottom: 2px solid #e9ecef; padding-bottom: 20px; margin-bottom: 30px; }
        .header h1 { color: #495057; margin: 0; font-size: 2em; }
        .nav { margin-bottom: 20px; }
        .nav a { color: #007bff; text-decoration: none; margin-right: 20px; }
        .nav a:hover { text-decoration: underline; }
        .editor-layout { display: flex; gap: 20px; }
        .file-list { flex: 0 0 250px; }
        .file-list h3 { margin-top: 0; color: #495057; }
        .file-list ul { list-style: none; padding: 0; }
        .file-list li { margin-bottom: 5px; }
        .file-list a { color: #007bff; text-decoration: none; display: block; padding: 8px; border-radius: 4px; }
        .file-list a:hover, .file-list a.active { background-color: #e9ecef; }
        .editor { flex: 1; }
        .editor textarea {
            width: 100%;
            height: 600px;
            font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 14px;
            line-height: 1.5;
            padding: 20px;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            background-color: var(--card-bg);
            color: var(--text-color);
            resize: vertical;
            transition: border-color 0.3s, box-shadow 0.3s;
        }
        .editor textarea:focus {
            outline: none;
            border-color: var(--button-bg);
            box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.1);
        }
        [data-theme="dark"] .editor textarea {
            background-color: #2d3748;
            color: #e9ecef;
        }
        .message { margin: 15px 0; padding: 10px; border-radius: 4px; }
        .success { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .save-btn { background-color: #28a745; color: white; border: none; padding: 12px 24px; border-radius: 4px; cursor: pointer; font-size: 16px; }
        .save-btn:hover { background-color: #218838; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>SQL Editor</h1>
        </div>
        <div class="nav">
            <a href="/">← Back to Pipeline</a>
            <a href="/json-editor">JSON Editor</a>
            <a href="/upload">Upload Files</a>
        </div>

        <div class="editor-layout">
            <div class="file-list">
                <h3>SQL Files</h3>
                <div class="file-tree">
                    {% for folder, files in file_tree.items() %}
                        <div class="folder-item">
                            <div class="folder-header">
                                {{ folder if folder != 'root' else 'Root Directory' }}
                            </div>
                            <ul class="file-list">
                                {% for file in files %}
                                    <li class="file-item {{ 'active' if file == selected_file else '' }}">
                                        <a href="?file={{ file }}" class="file-link">{{ file.split('/')[-1] }}</a>
                                        <form method="post" class="delete-form" onsubmit="return confirm('Delete {{ file.split('/')[-1] }}?')">
                                            <input type="hidden" name="delete" value="{{ file }}">
                                            <button type="submit" class="delete-btn" title="Delete file">Delete</button>
                                        </form>
                                    </li>
                                {% endfor %}
                            </ul>
                        </div>
                    {% endfor %}
                </div>
            </div>
            <div class="editor">
                {% if message %}
                <div class="message {{ 'success' if 'successfully' in message else 'error' }}">
                    {{ message }}
                </div>
                {% endif %}

                {% if selected_file %}
                <form method="post">
                    <h3>Editing: {{ selected_file }}</h3>
                    <textarea name="content" placeholder="Enter SQL content here...">{{ content }}</textarea>
                    <br><br>
                    <button type="submit" class="save-btn">Save Changes</button>
                </form>
                {% else %}
                <p>Select a SQL file from the list to edit.</p>
                {% endif %}
            </div>
        </div>
    </div>
</body>
</html>
"""

JSON_EDITOR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>JSON Editor - Database ETL Pipeline</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; margin: 0; padding: 20px; background-color: #f8f9fa; }
        .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 30px; }
        .header { border-bottom: 2px solid #e9ecef; padding-bottom: 20px; margin-bottom: 30px; }
        .header h1 { color: #495057; margin: 0; font-size: 2em; }
        .nav { margin-bottom: 20px; }
        .nav a { color: #007bff; text-decoration: none; margin-right: 20px; }
        .nav a:hover { text-decoration: underline; }
        .editor-layout { display: flex; gap: 20px; }
        .file-list { flex: 0 0 250px; }
        .file-list h3 { margin-top: 0; color: #495057; }
        .file-tree { margin-top: 15px; }
        .folder-item { margin-bottom: 20px; }
        .folder-header { font-weight: bold; color: #495057; background-color: #f8f9fa; padding: 10px; border-radius: 6px; border: 1px solid #dee2e6; margin-bottom: 8px; }
        .file-list { list-style: none; padding: 0; margin: 0; }
        .file-item { display: flex; align-items: center; padding: 8px 12px; border-radius: 4px; margin-bottom: 4px; background-color: #fff; border: 1px solid #e9ecef; }
        .file-item:hover { background-color: #f8f9fa; }
        .file-item.active { background-color: #e3f2fd; border-color: #2196f3; }
        .file-link { color: #007bff; text-decoration: none; flex: 1; }
        .file-link:hover { color: #0056b3; }
        .delete-form { margin: 0; }
        .delete-btn { background: none; border: none; color: #dc3545; cursor: pointer; padding: 4px; border-radius: 3px; font-size: 14px; }
        .delete-btn:hover { background-color: #f8d7da; }
        .file-tree { margin-top: 15px; }
        .folder-item { margin-bottom: 20px; }
        .folder-header { font-weight: bold; color: #495057; background-color: #f8f9fa; padding: 10px; border-radius: 6px; border: 1px solid #dee2e6; margin-bottom: 8px; }
        .file-list { list-style: none; padding: 0; margin: 0; }
        .file-item { display: flex; align-items: center; padding: 8px 12px; border-radius: 4px; margin-bottom: 4px; background-color: #fff; border: 1px solid #e9ecef; }
        .file-item:hover { background-color: #f8f9fa; }
        .file-item.active { background-color: #e3f2fd; border-color: #2196f3; }
        .file-link { color: #007bff; text-decoration: none; flex: 1; }
        .file-link:hover { color: #0056b3; }
        .delete-form { margin: 0; }
        .delete-btn { background: none; border: none; color: #dc3545; cursor: pointer; padding: 4px; border-radius: 3px; font-size: 14px; }
        .delete-btn:hover { background-color: #f8d7da; }
        .editor { flex: 1; }
        .editor textarea {
            width: 100%;
            height: 600px;
            font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 14px;
            line-height: 1.5;
            padding: 20px;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            background-color: var(--card-bg);
            color: var(--text-color);
            resize: vertical;
            transition: border-color 0.3s, box-shadow 0.3s;
        }
        .editor textarea:focus {
            outline: none;
            border-color: var(--button-bg);
            box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.1);
        }
        [data-theme="dark"] .editor textarea {
            background-color: #2d3748;
            color: #e9ecef;
        }
        .message { margin: 15px 0; padding: 10px; border-radius: 4px; }
        .success { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .save-btn { background-color: #28a745; color: white; border: none; padding: 12px 24px; border-radius: 4px; cursor: pointer; font-size: 16px; }
        .save-btn:hover { background-color: #218838; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>JSON Editor</h1>
        </div>
        <div class="nav">
            <a href="/">← Back to Pipeline</a>
            <a href="/sql-editor">SQL Editor</a>
            <a href="/upload">Upload Files</a>
        </div>

        <div class="editor-layout">
            <div class="file-list">
                <h3>JSON Files</h3>
                <div class="file-tree">
                    {% for folder, files in file_tree.items() %}
                        <div class="folder-item">
                            <div class="folder-header">
                                {{ folder if folder != 'root' else 'Root Directory' }}
                            </div>
                            <ul class="file-list">
                                {% for file in files %}
                                    <li class="file-item {{ 'active' if file == selected_file else '' }}">
                                        <a href="?file={{ file }}" class="file-link">{{ file.split('/')[-1] }}</a>
                                        <form method="post" class="delete-form" onsubmit="return confirm('Delete {{ file.split('/')[-1] }}?')">
                                            <input type="hidden" name="delete" value="{{ file }}">
                                            <button type="submit" class="delete-btn" title="Delete file">Delete</button>
                                        </form>
                                    </li>
                                {% endfor %}
                            </ul>
                        </div>
                    {% endfor %}
                </div>
            </div>
            <div class="editor">
                {% if message %}
                <div class="message {{ 'success' if 'successfully' in message else 'error' }}">
                    {{ message }}
                </div>
                {% endif %}

                {% if selected_file %}
                <form method="post">
                    <h3>Editing: {{ selected_file }}</h3>
                    <textarea name="content" placeholder="Enter JSON content here...">{{ content }}</textarea>
                    <br><br>
                    <button type="submit" class="save-btn">Save Changes</button>
                </form>
                {% else %}
                <p>Select a JSON file from the list to edit.</p>
                {% endif %}
            </div>
        </div>
    </div>
</body>
</html>
"""

UPLOAD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Upload Files - Database ETL Pipeline</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; margin: 0; padding: 20px; background-color: #f8f9fa; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 40px; }
        .header { text-align: center; border-bottom: 2px solid #e9ecef; padding-bottom: 20px; margin-bottom: 30px; }
        .header h1 { color: #495057; margin: 0; }
        .nav { margin-bottom: 30px; }
        .nav a { color: #007bff; text-decoration: none; margin-right: 20px; }
        .nav a:hover { text-decoration: underline; }
        .upload-form { margin-bottom: 30px; }
        .form-group { margin-bottom: 20px; }
        .form-group label { display: block; margin-bottom: 8px; font-weight: 500; color: #495057; }
        .form-group select, .form-group input[type="file"] { width: 100%; padding: 10px; border: 1px solid #ced4da; border-radius: 4px; font-size: 16px; }
        .upload-btn { background-color: #007bff; color: white; border: none; padding: 12px 24px; border-radius: 4px; cursor: pointer; font-size: 16px; }
        .upload-btn:hover { background-color: #0056b3; }
        .message { margin: 15px 0; padding: 10px; border-radius: 4px; }
        .success { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Upload Files</h1>
        </div>
        <div class="nav">
            <a href="/">← Back to Pipeline</a>
            <a href="/sql-editor">SQL Editor</a>
            <a href="/json-editor">JSON Editor</a>
        </div>

        {% if message %}
        <div class="message {{ 'success' if 'successfully' in message else 'error' }}">
            {{ message }}
        </div>
        {% endif %}

        <div class="upload-form">
            <form method="post" enctype="multipart/form-data">
                <div class="form-group">
                    <label for="type">File Type:</label>
                    <select name="type" id="type" required onchange="updateFolderPlaceholder()">
                        <option value="">Select type...</option>
                        <option value="sql">SQL File</option>
                        <option value="json">JSON Configuration</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="filename">Custom Filename (optional):</label>
                    <input type="text" name="filename" id="filename" placeholder="e.g., my_query.sql or config.json">
                    <small style="color: #6c757d; display: block; margin-top: 5px;">Leave empty to use original filename. Extension will be added automatically.</small>
                </div>
                <div class="form-group">
                    <label for="folder">Folder Path (optional):</label>
                    <input type="text" name="folder" id="folder" placeholder="e.g., custom/extract or my_configs">
                    <small style="color: #6c757d; display: block; margin-top: 5px;">Subfolder within sql/ or config/. Will be created if it doesn't exist.</small>
                </div>
                <div class="form-group">
                    <label for="file">Select File:</label>
                    <input type="file" name="file" id="file" accept=".sql,.json" required>
                </div>
                <button type="submit" class="upload-btn">Upload File</button>
            </form>
        </div>

        <script>
            function updateFolderPlaceholder() {
                const typeSelect = document.getElementById('type');
                const folderInput = document.getElementById('folder');
                const selectedType = typeSelect.value;

                if (selectedType === 'sql') {
                    folderInput.placeholder = 'e.g., extract, custom/queries, analytics';
                } else if (selectedType === 'json') {
                    folderInput.placeholder = 'e.g., tasks, custom/configs, workflows';
                } else {
                    folderInput.placeholder = 'e.g., custom/folder';
                }
            }
        </script>

        <div>
            <h3>File Upload Guidelines:</h3>
            <ul>
                <li><strong>Custom Filenames:</strong> Specify your own filename or leave empty to use the uploaded file's name</li>
                <li><strong>Folder Organization:</strong> Create subfolders to categorize your files (e.g., <code>analytics/queries</code>)</li>
                <li><strong>SQL Files:</strong> Stored in <code>sql/</code> directory with custom folder structure</li>
                <li><strong>JSON Files:</strong> Stored in <code>config/</code> directory with custom folder structure</li>
                <li><strong>Extensions:</strong> Automatically added if not included in custom filename</li>
                <li><strong>Overwrite:</strong> Existing files with the same name will be overwritten</li>
            </ul>
        </div>
    </div>
</body>
</html>
"""

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

    return render_template_string(HTML_TEMPLATE, status=status)

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

    return render_template_string(SQL_EDITOR_TEMPLATE, file_tree=file_tree, selected_file=selected_file, content=content, message=message)

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

    return render_template_string(JSON_EDITOR_TEMPLATE, file_tree=file_tree, selected_file=selected_file, content=content, message=message)

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
                return render_template_string(UPLOAD_TEMPLATE, message=message)

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

    return render_template_string(UPLOAD_TEMPLATE, message=message)

if __name__ == '__main__':
    print("Starting Database ETL Pipeline Web UI...")
    print("Open http://localhost:5000 in your browser")
    print("For task visualization, start Luigi daemon: luigid --background --logdir logs/")
    app.run(debug=True, host='0.0.0.0', port=5000)