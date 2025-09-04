async function generateFromSingleFile() {
  const file = document.getElementById("single-file-input").files[0];
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
  if (data && (data.kind === "quote_compare" || data.mode === "quote_compare")) {
    renderQuoteCompare(data);
    setStatus && setStatus('Done');
    return;
  }
  // NEW: Insights fallback (single-file track with no B/A and no recognizable line items)
  if (data && (data.kind === "insights" || data.mode === "insights")) {
    renderInsights(data.insights || {});
    if (data.message) renderNotice(data.message);
    setStatus && setStatus('Done');
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
    renderSingleFileError('No budget/actuals found. Showing file-level insights instead.');
  }
  if (data && data.diagnostics) {
    renderDiagnostics(data.diagnostics);
  }
}

// NEW: render workbook insights (cards + simple tables)
function renderInsights(ins) {
  const root = document.getElementById('results') || document.body;
  const box = document.getElementById('result_box') || root;
  box.innerHTML = '';
  const h = document.createElement('div');
  h.className = 'card';
  const highlights = (ins && ins.highlights && ins.highlights.length)
    ? ('<ul>' + ins.highlights.map(x => `<li>${escapeHtml(x)}</li>`).join('') + '</ul>')
    : '<p class="muted">No highlights available.</p>';
  const cards = (ins && ins.cards)
    ? ins.cards.map(c => `<div class="kv"><b>${escapeHtml(c.title || c.sheet || 'Metric')}</b>: ${escapeHtml(String(c.value_sar ?? c.value ?? '—'))}</div>`).join('')
    : '';
  h.innerHTML = `<div class="card-title">Workbook Insights</div>${highlights}${cards}`;
  box.appendChild(h);

  // Simple table renderers (top vendors, top items, spreads)
  function appendTable(title, rows, cols) {
    if (!rows || !rows.length) return;
    const div = document.createElement('div');
    div.className = 'card';
    const th = cols.map(c => `<th>${escapeHtml(c)}</th>`).join('');
    const trs = rows.slice(0, 20).map(r => `<tr>${cols.map(c => `<td>${escapeHtml(String(r[c] ?? ''))}</td>`).join('')}</tr>`).join('');
    div.innerHTML = `<div class="card-title">${escapeHtml(title)}</div><table class="simple"><thead><tr>${th}</tr></thead><tbody>${trs}</tbody></table>`;
    box.appendChild(div);
  }
  appendTable('Top vendors by spend', (ins.tables && ins.tables['workbook::vendor_totals']) || [], ['vendor','total_sar']);
  appendTable('Largest bid spreads', (ins.tables && ins.tables['workbook::vendor_spreads']) || [], ['description','min_vendor','min_unit_sar','max_vendor','max_unit_sar','spread_pct']);
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

// --- NEW: quote-compare rendering ---

function renderQuoteCompare(data) {
  const root = document.getElementById('results') || document.body;
  root.innerHTML = '';

  // Accept either "variance_items" or "spreads"
  const rows = Array.isArray(data.variance_items) && data.variance_items.length
    ? data.variance_items
    : (Array.isArray(data.spreads) ? data.spreads : []);
  if (rows.length) {
    const table = document.createElement('table');
    const head = document.createElement('thead');
    head.innerHTML = "<tr><th>Item</th><th>Min Vendor</th><th>Min Unit</th><th>Max Vendor</th><th>Max Unit</th><th>% Spread</th><th>Total Spread</th></tr>";
    table.appendChild(head);
    const body = document.createElement('tbody');
    rows.forEach(v => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${v.item_code ?? ""} ${v.description ?? ""}</td>
        <td>${v.min_vendor ?? ""}</td>
        <td>${v.min_unit_sar ?? ""}</td>
        <td>${v.max_vendor ?? ""}</td>
        <td>${v.max_unit_sar ?? ""}</td>
        <td>${v.spread_pct ?? ""}%</td>
        <td>${v.total_spread_sar ?? ""}</td>
      `.trim();
      body.appendChild(tr);
    });
    table.appendChild(body);
    root.appendChild(table);
  }

  // Vendor totals (accept array or map)
  const vtArray = Array.isArray(data.vendor_totals)
    ? data.vendor_totals.map(r => ({ vendor_name: r.vendor_name ?? r.vendor ?? "", total_amount_sar: r.total_amount_sar ?? r.total ?? r.amount ?? 0 }))
    : Object.entries(data.vendor_totals || {}).map(([vendor_name, total_amount_sar]) => ({ vendor_name, total_amount_sar }));
  if (vtArray.length) {
    const h = document.createElement('h3');
    h.textContent = 'Vendor Totals';
    root.appendChild(h);
    const table2 = document.createElement('table');
    const head2 = document.createElement('thead');
    head2.innerHTML = '<tr><th>Vendor</th><th>Total</th></tr>';
    table2.appendChild(head2);
    const body2 = document.createElement('tbody');
    vtArray.forEach(({ vendor_name, total_amount_sar }) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${vendor_name}</td><td>${total_amount_sar}</td>`;
      body2.appendChild(tr);
    });
    table2.appendChild(body2);
    root.appendChild(table2);
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

