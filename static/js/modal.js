/*
Modal and dialog functionality
*/

function confirmDelete(fileName, filePath) {
    const modal = document.createElement('div');
    modal.className = 'delete-modal';
    modal.innerHTML = `
        <div class="delete-modal-overlay" onclick="closeDeleteModal()"></div>
        <div class="delete-modal-content">
            <div class="delete-modal-header">
                <h3>Confirm Deletion</h3>
            </div>
            <div class="delete-modal-body">
                <p>Are you sure you want to delete the file <strong>"${fileName}"</strong>?</p>
                <p class="delete-warning">This action cannot be undone.</p>
            </div>
            <div class="delete-modal-footer">
                <button class="delete-modal-btn cancel-btn" onclick="closeDeleteModal()">Cancel</button>
                <form method="post" style="display: inline;">
                    <input type="hidden" name="delete" value="${filePath}">
                    <button type="submit" class="delete-modal-btn confirm-btn">Delete File</button>
                </form>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    // Focus the cancel button for accessibility
    setTimeout(() => {
        modal.querySelector('.cancel-btn').focus();
    }, 100);
}

function closeDeleteModal() {
    const modal = document.querySelector('.delete-modal');
    if (modal) {
        modal.remove();
    }
}

// Scheduled Run Modal Functions
function showScheduledModal() {
    const modal = document.getElementById('scheduled-modal');
    modal.style.display = 'block';
}

function closeScheduledModal() {
    const modal = document.getElementById('scheduled-modal');
    modal.style.display = 'none';
}

function runScheduled(type) {
    closeScheduledModal();

    // Create a form to submit the scheduled request
    const form = document.createElement('form');
    form.method = 'POST';
    form.style.display = 'none';

    const actionInput = document.createElement('input');
    actionInput.type = 'hidden';
    actionInput.name = 'action';
    actionInput.value = type === 'incremental' ? 'scheduled_incremental' : 'scheduled_full';

    form.appendChild(actionInput);
    document.body.appendChild(form);
    form.submit();
}

// Close modal on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        closeDeleteModal();
        closeScheduledModal();
    }
});