// Single-file: call /singlefile/analyze and render Procurement Summary cards when returned
function generateFromSingleFile() {
  const file = document.getElementById('single_file_input').files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append('file', file);
  fetch('/singlefile/analyze', { method: 'POST', body: fd })
    .then(r => r.json().then(j => ({ ok: r.ok, body: j })))
    .then(({ ok, body }) => {
      if (!ok) { showError(body.error || 'upload_failed'); return; }
      if (body.report_type === 'procurement_summary') {
        renderProcurementCards(body);
      } else if (body.report_type === 'variance_insights') {
        renderVarianceInsights(body);
      } else {
        showResultJSON(body);
      }
    })
    .catch(e => showError(e));
}

function renderProcurementCards(data) {
  const box = document.getElementById('result_box');
  if (!data.items || !data.items.length) { box.innerText = 'No procurement items found.'; return; }
  box.innerHTML = '';
  data.items.forEach(it => {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
      <div class="card-title">${it.item_code || '—'} — SAR ${it.amount_sar ?? '—'}</div>
      <div class="kv">Vendor: <b>${it.vendor || '—'}</b> &nbsp; | Date: ${it.doc_date || '—'}</div>
      <div class="kv">Qty/Unit: ${it.quantity || '—'} ${it.unit || ''} &nbsp; | Unit price: ${it.unit_price_sar ?? '—'}</div>
      <div class="desc">${(it.description || '').slice(0, 500)}</div>
      <div class="src">Source: ${it.source}</div>
    `;
    box.appendChild(div);
  });
}

function showResultJSON(data) {
  const box = document.getElementById('result_box');
  box.textContent = JSON.stringify(data, null, 2);
}

function showError(e) {
  const box = document.getElementById('result_box');
  box.textContent = 'Error: ' + (e.message || e);
}

function renderVarianceInsights(data) {
  const box = document.getElementById('result_box');
  if (!data.items || !data.items.length) { box.innerText = 'No variance items found.'; return; }
  box.innerHTML = '';
  data.items.forEach(it => {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
      <div class="card-title">${it.label || 'Line'}</div>
      <div class="kv">Budget: ${it.budget_sar} | Actual: ${it.actual_sar} | Variance: ${it.variance_sar}</div>`;
    box.appendChild(div);
  });
}
