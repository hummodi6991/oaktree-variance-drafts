async function generateFromSingleFile() {
  const file = document.getElementById('single_file_input').files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append('file', file);

  const resp = await fetch('/drafts/from-file', { method: 'POST', body: fd });
  let data = {};
  try { data = await resp.json(); } catch (_) {}
  if (!resp.ok) {
    renderSingleFileError((data && data.error) ? data.error : `Request failed (HTTP ${resp.status})`);
    return;
  }
  if (data && data.error) {
    renderSingleFileError(data.error);
    return;
  }

  const variance = (data.variance_items || []);
  const hasVariance = Array.isArray(variance) && variance.length > 0;
  const procurement = (data.procurement_summary && data.procurement_summary.items)
    ? data.procurement_summary.items : [];
  const hasProcurement = Array.isArray(procurement) && procurement.length > 0;

  if (hasVariance) {
    renderVarianceDraftCards(variance);
  } else if (hasProcurement) {
    if (data.message) renderNotice(data.message);
    renderProcurementSummary({ items: procurement, insights: data.insights || {} });
  } else {
    renderSingleFileError('No budget/actuals found and no recognizable procurement lines. Please upload budget/actuals CSV/Excel for variance, or a quote/BOQ for procurement.');
  }
}

function renderVarianceDraftCards(items) {
  const box = document.getElementById('result_box');
  if (!items.length) { box.innerText = 'No variance items found.'; return; }
  box.innerHTML = '';
  items.forEach(it => {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
      <div class="card-title">${it.label || 'Line'}</div>
      <div class="kv">Budget: ${it.budget_sar} | Actual: ${it.actual_sar} | Variance: ${it.variance_sar}</div>`;
    box.appendChild(div);
  });
}

function renderProcurementSummary({ items, insights }) {
  const box = document.getElementById('result_box');
  if (!items.length) { box.innerText = 'No procurement items found.'; return; }
  box.innerHTML = '';
  items.forEach(it => {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
      <div class="card-title">${it.item_code || '—'} — SAR ${it.amount_sar ?? '—'}</div>
      <div class="kv">Vendor: <b>${it.vendor_name || it.vendor || '—'}</b></div>
      <div class="kv">Qty/Unit: ${it.qty || '—'} ${it.unit || ''} | Unit price: ${it.unit_price_sar ?? '—'}</div>
      <div class="desc">${(it.description || '').slice(0, 500)}</div>`;
    box.appendChild(div);
  });

  if (insights && (insights.totals_per_vendor || insights.top_lines_by_amount)) {
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(insights, null, 2);
    box.appendChild(pre);
  }
}

function renderSingleFileError(msg) {
  const box = document.getElementById('result_box');
  box.innerText = msg;
}

function showError(e) {
  renderSingleFileError(e.message || e);
}

function renderNotice(msg) {
  try {
    const el = document.getElementById('notice') || (function(){
      const n = document.createElement('div');
      n.id = 'notice';
      n.className = 'notice info';
      document.body.prepend(n);
      return n;
    })();
    el.textContent = msg;
  } catch (_) {
    console.warn(msg);
  }
}

