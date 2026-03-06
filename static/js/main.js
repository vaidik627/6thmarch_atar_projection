// Atar Capital Prebid Analysis – JS helpers

// Format numbers with commas on analysis page
document.addEventListener('DOMContentLoaded', () => {

  // Auto-dismiss flash alerts after 5s
  document.querySelectorAll('.alert-dismissible').forEach(el => {
    setTimeout(() => {
      const btn = el.querySelector('.btn-close');
      if (btn) btn.click();
    }, 5000);
  });

  // Projection table: highlight negative cells red
  document.querySelectorAll('.projection-table td').forEach(td => {
    const text = td.textContent.trim().replace(/,/g, '');
    const num = parseFloat(text);
    if (!isNaN(num) && num < 0 && !td.classList.contains('text-danger')) {
      td.classList.add('text-danger');
    }
  });

});
