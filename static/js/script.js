/*
Copyright 2025 Jonas G Mwambimbi
Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0
*/

import './theme.js';
import './websocket.js';
import './file_manager.js';
import './modal.js';

document.addEventListener('DOMContentLoaded', function () {
    const savedTheme = localStorage.getItem('theme');
    const themeBtn = document.getElementById('theme-toggle');

    if (savedTheme === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
        themeBtn.textContent = '☀️ Light Mode';
    } else {
        themeBtn.textContent = '🌙 Dark Mode';
    }

    initializeWebSocket();

    if (document.getElementById('file-table')) {
        initializeFileTable();
    }
});