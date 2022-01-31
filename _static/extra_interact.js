
        function collapse(cell) {
        var row = cell.parentElement;
        var target_row = row.parentElement.children[row.rowIndex + 1];
        if (target_row.style.display == 'table-row') {
            cell.innerHTML = '<i class="fas fa-chevron-down"></i>';
            target_row.style.display = 'none';
        } else {
            cell.innerHTML = '<i class="fas fa-chevron-up"></i>';
            target_row.style.display = 'table-row';
        }
        }
        
