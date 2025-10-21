/*
WebSocket communication functionality
*/

let socket;

function initializeWebSocket() {
    socket = io();

    socket.on('connect', function() {
        console.log('Connected to server');
        // Request current status when connected
        socket.emit('get_status');
    });

    socket.on('task_update', function(data) {
        console.log('Received task update:', data);
        updateProgress(data);
    });

    socket.on('disconnect', function() {
        console.log('Disconnected from server');
    });

    // Request status periodically to ensure we catch running pipelines
    setInterval(function() {
        if (socket.connected) {
            socket.emit('get_status');
        }
    }, 5000); // Check every 5 seconds
}

function updateProgress(data) {
    const progressSection = document.getElementById('progress-section');
    const statusPanel = document.getElementById('status-panel');
    const statusContent = document.getElementById('status-content');

    // Handle running pipeline
    if (data.running) {
        // Hide status panel and show progress section
        statusPanel.style.display = 'none';
        progressSection.style.display = 'block';

        const progressFill = document.getElementById('progress-fill');
        const progressPercent = document.getElementById('progress-percent');
        const progressMessage = document.getElementById('progress-message');
        const progressStatus = document.getElementById('progress-status');

        // Update progress bar
        if (data.progress !== undefined) {
            progressFill.style.width = data.progress + '%';
            progressPercent.textContent = data.progress + '%';
        }

        // Update message
        if (data.message) {
            progressMessage.textContent = data.message;
        }

        // Update status/result
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
        // Pipeline not running - hide progress and show status if there's a result
        progressSection.style.display = 'none';

        if (data.result) {
            // Show status panel with final result
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
            // No result to show, hide status panel
            statusPanel.style.display = 'none';
        }
    }

    // Disable/enable buttons based on running state
    const buttons = ['full_refresh_btn', 'incremental_btn', 'scheduled_btn', 'force_refresh_btn'];
    buttons.forEach(btnId => {
        const btn = document.getElementById(btnId);
        if (btn) {
            btn.disabled = data.running || false;
        }
    });
}