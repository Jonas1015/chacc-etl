/*
Theme management functionality
*/

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