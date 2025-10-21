/*
File management functionality for editors
*/

function initializeFileTable() {
    const fileTables = document.querySelectorAll('#file-table');

    fileTables.forEach(table => {
        const container = table.closest('.editor-layout');
        const searchInput = container.querySelector('#file-search');
        const pageSizeSelect = container.querySelector('#page-size');
        const fileTableBody = container.querySelector('#file-table-body');
        const prevBtn = container.querySelector('#prev-btn');
        const nextBtn = container.querySelector('#next-btn');
        const paginationInfo = container.querySelector('#pagination-info');
        const fileCount = container.querySelector('#file-count');
        const editor = container.querySelector('.editor');
        const fileList = container.querySelector('.file-list');
        const expandBtn = container.querySelector('#expand-btn');

        let currentPage = 1;
        let pageSize = parseInt(pageSizeSelect.value);
        let filteredRows = [];
        let allRows = Array.from(fileTableBody.querySelectorAll('.file-row'));
        let isEditorExpanded = false;

        updateFilteredRows();
        updateDisplay();

        searchInput.addEventListener('input', function() {
            updateFilteredRows();
            currentPage = 1;
            updateDisplay();
        });

        pageSizeSelect.addEventListener('change', function() {
            pageSize = parseInt(this.value);
            currentPage = 1;
            updateDisplay();
        });

        prevBtn.addEventListener('click', function() {
            if (currentPage > 1) {
                currentPage--;
                updateDisplay();
            }
        });

        nextBtn.addEventListener('click', function() {
            const maxPages = Math.ceil(filteredRows.length / pageSize);
            if (currentPage < maxPages) {
                currentPage++;
                updateDisplay();
            }
        });

        function toggleEditor() {
            console.log('Toggle editor called, current state:', isEditorExpanded);
            isEditorExpanded = !isEditorExpanded;

            if (isEditorExpanded) {
                console.log('Expanding editor');
                fileList.style.display = 'none';
                editor.style.width = '100%';
                editor.style.gridColumn = '1 / -1';
                expandBtn.innerHTML = `
                    <svg fill="#ffffff" width="16" height="16" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                        <path d="M6,2.5 C6,2.22385763 6.22385763,2 6.5,2 C6.77614237,2 7,2.22385763 7,2.5 L7,4.5 C7,5.88071187 5.88071187,7 4.5,7 L2.5,7 C2.22385763,7 2,6.77614237 2,6.5 C2,6.22385763 2.22385763,6 2.5,6 L4.5,6 C5.32842712,6 6,5.32842712 6,4.5 L6,2.5 Z M2.5,18 C2.22385763,18 2,17.7761424 2,17.5 C2,17.2238576 2.22385763,17 2.5,17 L4.5,17 C5.88071187,17 7,18.1192881 7,19.5 L7,21.5 C7,21.7761424 6.77614237,22 6.5,22 C6.22385763,22 6,21.7761424 6,21.5 L6,19.5 C6,18.6715729 5.32842712,18 4.5,18 L2.5,18 Z M21.5,6 C21.7761424,6 22,6.22385763 22,6.5 C22,6.77614237 21.7761424,7 21.5,7 L19.5,7 C18.1192881,7 17,5.88071187 17,4.5 L17,2.5 C17,2.22385763 17.2238576,2 17.5,2 C17.7761424,2 18,2.22385763 18,2.5 L18,4.5 C18,5.32842712 18.6715729,6 19.5,6 L21.5,6 Z M18,21.5 C18,21.7761424 17.7761424,22 17.5,22 C17.2238576,22 17,21.7761424 17,21.5 L17,19.5 C17,18.1192881 18.1192881,17 19.5,17 L21.5,17 C21.7761424,17 22,17.2238576 22,17.5 C22,17.7761424 21.7761424,18 21.5,18 L19.5,18 C18.6715729,18 18,18.6715729 18,19.5 L18,21.5 Z"/>
                    </svg>
                `;
                expandBtn.title = 'Collapse Editor';
                expandBtn.classList.add('expanded');
            } else {
                console.log('Collapsing editor');
                editor.style.width = '';
                editor.style.gridColumn = '2';
                fileList.style.display = '';
                expandBtn.innerHTML = `
                    <svg fill="#ffffff" width="16" height="16" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                        <path d="M4,7.5 C4,7.77614237 3.77614237,8 3.5,8 C3.22385763,8 3,7.77614237 3,7.5 L3,5.5 C3,4.11928813 4.11928813,3 5.5,3 L7.5,3 C7.77614237,3 8,3.22385763 8,3.5 C8,3.77614237 7.77614237,4 7.5,4 L5.5,4 C4.67157288,4 4,4.67157288 4,5.5 L4,7.5 Z M16.5,4 C16.2238576,4 16,3.77614237 16,3.5 C16,3.22385763 16.2238576,3 16.5,3 L18.5,3 C19.8807119,3 21,4.11928813 21,5.5 L21,7.5 C21,7.77614237 20.7761424,8 20.5,8 C20.2238576,8 20,7.77614237 20,7.5 L20,5.5 C20,4.67157288 19.3284271,4 18.5,4 L16.5,4 Z M20,16.5 C20,16.2238576 20.2238576,16 20.5,16 C20.7761424,16 21,16.2238576 21,16.5 L21,18.5 C21,19.8807119 19.8807119,21 18.5,21 L16.5,21 C16.2238576,21 16,20.7761424 16,20.5 C16,20.2238576 16.2238576,20 16.5,20 L18.5,20 C19.3284271,20 20,19.3284271 20,18.5 L20,16.5 Z M7.5,20 C7.77614237,20 8,20.2238576 8,20.5 C8,20.7761424 7.77614237,21 7.5,21 L5.5,21 C4.11928813,21 3,19.8807119 3,18.5 L3,16.5 C3,16.2238576 3.22385763,16 3.5,16 C3.77614237,16 4,16.2238576 4,16.5 L4,18.5 C4,19.3284271 4.67157288,20 5.5,20 L7.5,20 Z"/>
                    </svg>
                `;
                expandBtn.title = 'Expand Editor';
                expandBtn.classList.remove('expanded');
            }
        }

        if (expandBtn) {
            console.log('Expand button found, adding event listener');
            expandBtn.addEventListener('click', function(e) {
                console.log('Expand button clicked');
                e.preventDefault();
                toggleEditor();
            });
        } else {
            console.log('Expand button not found');
        }

        function updateFilteredRows() {
            const searchTerm = searchInput.value.toLowerCase().trim();

            if (searchTerm === '') {
                filteredRows = allRows;
            } else {
                filteredRows = allRows.filter(row => {
                    const fileName = row.querySelector('.file-name-cell').textContent.toLowerCase();
                    const folderName = row.querySelector('.file-folder-cell').textContent.toLowerCase();
                    return fileName.includes(searchTerm) || folderName.includes(searchTerm);
                });
            }
        }

        function updateDisplay() {
            const startIndex = (currentPage - 1) * pageSize;
            const endIndex = startIndex + pageSize;
            const maxPages = Math.ceil(filteredRows.length / pageSize);

            allRows.forEach(row => row.style.display = 'none');

            filteredRows.slice(startIndex, endIndex).forEach(row => {
                row.style.display = '';
            });

            prevBtn.disabled = currentPage === 1;
            nextBtn.disabled = currentPage >= maxPages;

            if (filteredRows.length === 0) {
                paginationInfo.textContent = 'No files found';
            } else {
                paginationInfo.textContent = `Page ${currentPage} of ${maxPages}`;
            }

            fileCount.textContent = `${filteredRows.length} files`;
        }
    });
}