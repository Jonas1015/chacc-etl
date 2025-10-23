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
        const interruptBtn = document.getElementById('interrupt-btn');
        if (interruptBtn) {
            interruptBtn.style.display = data.running ? 'inline-block' : 'none';
        }
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

    if (data.luigi_status === 'not_running') {
        progressSection.style.display = 'none';
        statusPanel.style.display = 'block';
        statusContent.textContent = data.message;
        statusContent.className = 'status-content error';

        const buttons = ['full_refresh_btn', 'incremental_btn', 'scheduled_btn', 'force_refresh_btn'];
        buttons.forEach(btnId => {
            const btn = document.getElementById(btnId);
            if (btn) {
                btn.disabled = true;
                btn.style.opacity = '0.5';
            }
        });
        return;
    }

    if (data.luigi_status === 'pending') {
        progressSection.style.display = 'none';
        statusPanel.style.display = 'block';
        statusContent.textContent = data.message;
        statusContent.className = 'status-content info';

        // Enable buttons when pipeline is pending (ready to start)
        const buttons = ['full_refresh_btn', 'incremental_btn', 'scheduled_btn', 'force_refresh_btn'];
        buttons.forEach(btnId => {
            const btn = document.getElementById(btnId);
            if (btn) {
                btn.disabled = false;
                btn.style.opacity = '1';
            }
        });
        return;
    }

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

        // Show current task name prominently
        if (data.current_task) {
            const taskDisplay = document.createElement('div');
            taskDisplay.className = 'current-task-display';
            taskDisplay.innerHTML = `
                <strong>Current Task:</strong> ${data.current_task}<br>
                <small>Elapsed: ${data.message ? data.message.match(/(\d+)s elapsed/)?.[1] || '0' : '0'}s</small>
            `;

            // Update or create the task display
            let existingTaskDisplay = progressSection.querySelector('.current-task-display');
            if (existingTaskDisplay) {
                existingTaskDisplay.replaceWith(taskDisplay);
            } else {
                progressSection.insertBefore(taskDisplay, progressSection.querySelector('.progress-container'));
            }
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

        // Remove current task display when not running
        const taskDisplay = progressSection.querySelector('.current-task-display');
        if (taskDisplay) {
            taskDisplay.remove();
        }

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
        } else if (data.luigi_status === 'ready') {
            // Show ready status when Luigi is running but no pipeline active
            statusPanel.style.display = 'block';
            statusContent.textContent = data.message;
            statusContent.className = 'status-content info';
        } else {
            statusPanel.style.display = 'none';
        }
    }

    // Enable/disable buttons based on Luigi status and pipeline state
    // Force refresh button is always enabled (except when Luigi is not running)
    const buttons = ['full_refresh_btn', 'incremental_btn', 'scheduled_btn'];
    const shouldDisable = data.luigi_status === 'not_running' || data.running;

    buttons.forEach(btnId => {
        const btn = document.getElementById(btnId);
        if (btn) {
            btn.disabled = shouldDisable;
            btn.style.opacity = shouldDisable ? '0.5' : '1';
        }
    });

    // Force refresh button is always enabled when Luigi is running
    const forceBtn = document.getElementById('force_refresh_btn');
    if (forceBtn) {
        const forceShouldDisable = data.luigi_status === 'not_running';
        forceBtn.disabled = forceShouldDisable;
        forceBtn.style.opacity = forceShouldDisable ? '0.5' : '1';
    }
}