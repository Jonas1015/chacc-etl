
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