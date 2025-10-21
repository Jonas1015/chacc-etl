/*
WebSocket communication functionality
*/

let socket;

function initializeWebSocket() {
    socket = io();

    socket.on('connect', function() {
        console.log('Connected to server');
        socket.emit('get_status');
    });

    socket.on('task_update', function(data) {
        console.log('Received task update:', data);
        updateProgress(data);
    });

    socket.on('disconnect', function() {
        console.log('Disconnected from server');
    });

    setInterval(function() {
        if (socket.connected) {
            socket.emit('get_status');
        }
    }, 5000);
}

function updateProgress(data) {
    const progressSection = document.getElementById('progress-section');
    const statusPanel = document.getElementById('status-panel');
    const statusContent = document.getElementById('status-content');

    if (data.running) {
        statusPanel.style.display = 'none';
        progressSection.style.display = 'block';

        const progressFill = document.getElementById('progress-fill');
        const progressPercent = document.getElementById('progress-percent');
        const progressMessage = document.getElementById('progress-message');
        const progressStatus = document.getElementById('progress-status');

        if (data.progress !== undefined) {
            progressFill.style.width = data.progress + '%';
            progressPercent.textContent = data.progress + '%';
        }

        if (data.message) {
            progressMessage.textContent = data.message;
        }

        if (data.result) {
            progressStatus.textContent = data.result;
            progressStatus.className = 'progress-status';

            if (data.result.includes('successfully')) {
                progressStatus.classList.add('success');
            } else if (data.result.includes('failed') || data.result.includes('Error')) {
                progressStatus.classList.add('error');
            } else {
                progressStatus.classList.add('info');
            }
        }
    } else {
        progressSection.style.display = 'none';

        if (data.result) {
            statusPanel.style.display = 'block';
            statusContent.textContent = data.result;
            statusContent.className = 'status-content';

            if (data.result.includes('successfully')) {
                statusContent.classList.add('success');
            } else if (data.result.includes('failed') || data.result.includes('Error')) {
                statusContent.classList.add('error');
            } else {
                statusContent.classList.add('info');
            }
        } else {
            statusPanel.style.display = 'none';
        }
    }

    const buttons = ['full_refresh_btn', 'incremental_btn', 'scheduled_btn', 'force_refresh_btn'];
    buttons.forEach(btnId => {
        const btn = document.getElementById(btnId);
        if (btn) {
            btn.disabled = data.running || false;
        }
    });
}