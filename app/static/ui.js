// Single-file: call /drafts/from-file and render cards when returned
async function generateFromSingleFile() {
  const file = document.getElementById('single_file_input').files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append('file', file);
  try {
    const resp = await fetch('/drafts/from-file', { method: 'POST', body: fd });
    let data = {};
    try { data = await resp.json(); } catch (_) {}
    if (data.error) {
      renderSingleFileError(data.error);
      return;
    }
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    if (data.report_type === 'procurement_summary') {
      renderProcurementCards(data);
    } else if (data.report_type === 'variance_insights') {
      renderVarianceInsights(data);
    } else {
      showResultJSON(data);
    }
  } catch (e) {
    showError(e);
  }
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

function renderSingleFileError(e) {
  showError(e);
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
