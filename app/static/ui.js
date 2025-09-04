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
    if (data && data.diagnostics) { renderDiagnostics(data.diagnostics); }
    return;
  }
  if (data && data.error) {
    renderSingleFileError(data.error);
    if (data.diagnostics) { renderDiagnostics(data.diagnostics); }
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
  if (data && data.diagnostics) {
    renderDiagnostics(data.diagnostics);
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

function renderDiagnostics(diag) {
  const container = document.getElementById('results') || document.body;
  const section = document.createElement('section');
  section.className = 'diagnostics';

  const details = document.createElement('details');
  details.open = false;
  const summary = document.createElement('summary');
  summary.textContent = 'Diagnostics';
  details.appendChild(summary);

  const meta = document.createElement('div');
  meta.innerHTML = `
    <div style="margin:8px 0;">
      <strong>Correlation ID:</strong> <code>${escapeHtml(diag.correlation_id || '')}</code><br/>
      <strong>Duration:</strong> ${Number(diag.duration_ms||0)} ms<br/>
      <strong>Sheets/Steps:</strong> ${Array.isArray(diag.events) ? diag.events.length : 0} events,
      <strong>Warnings:</strong> ${Array.isArray(diag.warnings) ? diag.warnings.length : 0}
    </div>
  `;
  details.appendChild(meta);

  if (Array.isArray(diag.warnings) && diag.warnings.length) {
    const w = document.createElement('div');
    w.innerHTML = `<strong>Warnings</strong>`;
    const ul = document.createElement('ul');
    diag.warnings.forEach(wrn => {
      const li = document.createElement('li');
      li.textContent = `[${wrn.code}] ${wrn.message || ''}`;
      ul.appendChild(li);
    });
    w.appendChild(ul);
    details.appendChild(w);
  }

  const pre = document.createElement('pre');
  pre.style.whiteSpace = 'pre-wrap';
  const json = JSON.stringify(diag, null, 2);
  pre.textContent = json;

  const bar = document.createElement('div');
  bar.style.display = 'flex';
  bar.style.gap = '8px';
  bar.style.margin = '8px 0';

  const copyBtn = document.createElement('button');
  copyBtn.textContent = 'Copy diagnostics JSON';
  copyBtn.onclick = async () => {
    try { await navigator.clipboard.writeText(json); toast('Copied diagnostics'); }
    catch (e) { toast('Copy failed'); }
  };
  bar.appendChild(copyBtn);

  details.appendChild(bar);
  details.appendChild(pre);
  section.appendChild(details);
  container.appendChild(section);
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;')
    .replace(/'/g,'&#039;');
}

function toast(msg) {
  const t = document.createElement('div');
  t.textContent = msg;
  t.style.position='fixed'; t.style.bottom='16px'; t.style.right='16px';
  t.style.background='#333'; t.style.color='#fff'; t.style.padding='8px 12px'; t.style.borderRadius='6px';
  t.style.zIndex=9999; t.style.opacity='0.95';
  document.body.appendChild(t);
  setTimeout(()=>t.remove(), 2000);
}

// --- Insights rendering ---
function renderResult(payload) {
  // existing rendering...
  if (!payload) return;
  if (payload.mode === 'insights' && payload.insights) {
    renderInsights(payload.insights);
  }
  if (payload.mode === 'quote_compare' && payload.insights && payload.insights.highlights && payload.insights.highlights.length) {
    const root = document.getElementById('results') || document.body;
    const box = document.createElement('div');
    box.style.marginTop = '8px';
    const h = document.createElement('h3'); h.textContent = 'Highlights'; box.appendChild(h);
    const ul = document.createElement('ul');
    payload.insights.highlights.forEach(t => { const li = document.createElement('li'); li.textContent = t; ul.appendChild(li); });
    box.appendChild(ul);
    root.appendChild(box);
  }
  if (payload.diagnostics) {
    renderDiagnostics(payload.diagnostics);
  }
}

function renderInsights(ins) {
  const root = document.getElementById('results') || document.body;
  const wrap = document.createElement('section');
  wrap.className = 'insights';

  // Highlights
  if (Array.isArray(ins.highlights) && ins.highlights.length) {
    const h = document.createElement('div');
    h.innerHTML = '<h3>Highlights</h3>';
    const ul = document.createElement('ul');
    ins.highlights.forEach(t => {
      const li = document.createElement('li'); li.textContent = t; ul.appendChild(li);
    });
    h.appendChild(ul);
    wrap.appendChild(h);
  }

  // KPI cards
  if (Array.isArray(ins.cards) && ins.cards.length) {
    const cards = document.createElement('div');
    cards.style.display = 'grid';
    cards.style.gridTemplateColumns = 'repeat(auto-fit, minmax(200px, 1fr))';
    cards.style.gap = '12px';
    ins.cards.forEach(c => {
      const card = document.createElement('div');
      card.style.border = '1px solid #ddd'; card.style.borderRadius = '8px'; card.style.padding = '10px';
      const title = document.createElement('div'); title.textContent = c.title || c.sheet || 'Metric'; title.style.fontWeight = '600';
      const val = document.createElement('div'); val.textContent = (c.value_sar !== undefined) ? `${c.value_sar} SAR` : (c.value || '');
      card.appendChild(title); card.appendChild(val); cards.appendChild(card);
    });
    wrap.appendChild(cards);
  }

  // Tables
  if (ins.tables && typeof ins.tables === 'object') {
    const keys = Object.keys(ins.tables);
    keys.forEach(name => {
      const tblData = ins.tables[name];
      const sec = document.createElement('details');
      sec.open = false;
      const sum = document.createElement('summary');
      sum.textContent = name;
      sec.appendChild(sum);
      const pre = document.createElement('pre');
      pre.style.whiteSpace = 'pre-wrap';
      pre.textContent = JSON.stringify(tblData, null, 2);
      sec.appendChild(pre);
      wrap.appendChild(sec);
    });
  }

  root.appendChild(wrap);
}

