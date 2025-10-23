
document.addEventListener('DOMContentLoaded', function() {
    const perPageSelect = document.getElementById('per_page');
    if (perPageSelect) {
        perPageSelect.addEventListener('change', function() {
            const urlParams = new URLSearchParams(window.location.search);

            urlParams.set('per_page', this.value);

            urlParams.set('page', '1');

            window.location.href = window.location.pathname + '?' + urlParams.toString();
        });
    }

    // Add click handlers for execution rows
    const executionRows = document.querySelectorAll('.execution-row');
    executionRows.forEach(row => {
        row.addEventListener('click', function() {
            const executionId = this.querySelector('.id-cell').textContent.trim();
            showPipelineTasksDialog(executionId);
        });
    });
});

function clearHistory() {
    if (confirm('Are you sure you want to clear all pipeline history? This action cannot be undone.')) {
        fetch('/clear-history', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    location.reload();
                } else {
                    alert('Failed to clear history: ' + data.message);
                }
            })
            .catch(error => {
                alert('Error clearing history: ' + error.message);
            });
    }
}

function showPipelineTasksDialog(pipelineId) {
    // Create modal dialog
    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>Pipeline Tasks - Execution #${pipelineId}</h3>
                <span class="modal-close">&times;</span>
            </div>
            <div class="modal-body">
                <div class="loading">Loading tasks...</div>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    // Show modal
    modal.style.display = 'block';

    // Close modal handlers
    const closeBtn = modal.querySelector('.modal-close');
    closeBtn.onclick = function() {
        modal.remove();
    };

    modal.onclick = function(event) {
        if (event.target === modal) {
            modal.remove();
        }
    };

    // Fetch and display tasks
    fetch(`/pipeline-tasks/${pipelineId}`)
        .then(response => response.json())
        .then(data => {
            const modalBody = modal.querySelector('.modal-body');
            if (data.tasks && data.tasks.length > 0) {
                const tasksHtml = `
                    <div class="tasks-table-container">
                        <table class="tasks-table">
                            <thead>
                                <tr>
                                    <th>Task Name</th>
                                    <th>Type</th>
                                    <th>Status</th>
                                    <th>Start Time</th>
                                    <th>Duration</th>
                                    <th>Records</th>
                                    <th>Error</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.tasks.map(task => `
                                    <tr class="${getTaskStatusClass(task.status)}">
                                        <td>${task.task_name}</td>
                                        <td>${task.task_type}</td>
                                        <td><span class="status-badge ${getTaskStatusClass(task.status)}">${task.status}</span></td>
                                        <td>${task.start_time ? new Date(task.start_time).toLocaleString() : 'N/A'}</td>
                                        <td>${task.duration_seconds ? task.duration_seconds + 's' : 'N/A'}</td>
                                        <td>${task.records_processed || 0}</td>
                                        <td>${task.error_message || ''}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                `;
                modalBody.innerHTML = tasksHtml;
            } else {
                modalBody.innerHTML = '<div class="empty-state"><p>No tasks found for this pipeline execution.</p></div>';
            }
        })
        .catch(error => {
            const modalBody = modal.querySelector('.modal-body');
            modalBody.innerHTML = `<div class="error-state"><p>Error loading tasks: ${error.message}</p></div>`;
        });
}

function getTaskStatusClass(status) {
    switch (status?.toUpperCase()) {
        case 'DONE': return 'success';
        case 'FAILED': return 'failed';
        case 'RUNNING': return 'pending';
        case 'DISABLED': return 'interrupted';
        case 'PENDING': return 'pending';
        default: return 'info';
    }
}