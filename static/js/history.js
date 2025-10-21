/*
History page specific functionality
*/

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