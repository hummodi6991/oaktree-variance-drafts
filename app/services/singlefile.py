from __future__ import annotations
import io, re, json
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
import chardet

try:
    import pdfplumber
except Exception:
    pdfplumber = None
try:
    import docx
except Exception:
    docx = None

# ----------------------------
# Helpers (no speculation)
# ----------------------------
BUDGET_KEYS   = {"budget","planned","plan","budget_sar","planned_sar","budget_amount"}
ACTUAL_KEYS   = {"actual","actuals","spent","actual_sar","actual_amount","spend_sar"}
PROJECT_KEYS  = {"project","project_id","project name","projectname","proj_id"}
PERIOD_KEYS   = {"period","month","month_year","posting_period","date"}
CODE_KEYS     = {"cost_code","gl_code","costcode","account_code","code","item no","item_no","item #","reference","ref"}
CAT_KEYS      = {"category","reporting_category","group","trade"}
DESC_KEYS     = {"description","desc","item","line_item","scope","description of works","item description","work description","specification"}
QTY_KEYS      = {"qty","quantity","qtty","qty.","no of doors","no of units","no.","q","qty (nos)","nos","units","quantity (nos)"}
UPRICE_KEYS   = {"unit_price","unit price","unit rate","rate","u.rate","unit_price_sar","price per unit","price/unit","u rate","unit rate (sar)","unit price (sar)"}
AMOUNT_KEYS   = {"amount","amount_sar","line_total","total","value","total_sar","total price","line amount","extended amount","net amount","subtotal","grand total","total price (sar)","total (sar)","line total (sar)"}
VENDOR_KEYS   = {"vendor","vendor_name","supplier","supplier_name","company","vendor/supplier","quoted by","bidder","vendor name"}

AMT_RE = r'(?:SAR|SR|\$)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)'
DATE_RE = r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{2}[/-]\d{2}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{4})'
ITEM_RE = r'\b(D0?\d+)\b'

def _norm_cols(df: pd.DataFrame) -> Dict[str,str]:
    m = {}
    for c in df.columns:
        k = str(c).strip().lower()
        k = re.sub(r"[\u200B-\u200D\uFEFF]", "", k)
        k = re.sub(r"[.\(\)\[\]]", " ", k)
        k = re.sub(r"[-_]+", " ", k)
        k = re.sub(r"\s+", " ", k).strip()
        if k in m:  # keep first occurrence
            continue
        m[k] = c
    return m

def _read_df(name: str, data: bytes) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".xlsx") or low.endswith(".xls"):
        try:
            return pd.read_excel(io.BytesIO(data))
        except Exception:
            return pd.DataFrame()
    enc = (chardet.detect(data) or {}).get("encoding") or "utf-8"
    try:
        return pd.read_csv(io.BytesIO(data), encoding=enc)
    except Exception:
        return pd.DataFrame()

def _read_excel_sheets(data: bytes) -> Dict[str, pd.DataFrame]:
    """Read all sheets (if Excel), otherwise return empty dict."""
    try:
        bio = io.BytesIO(data)
        xls = pd.ExcelFile(bio)
        return {sn: xls.parse(sn) for sn in xls.sheet_names}
    except Exception:
        return {}

def _coerce_header_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Many quotes put headers on row 2/3 with a title block above.
    Find the first row that *looks* like a header (>=2 header-like cells) and
    promote it to header.
    """
    if df.empty:
        return df
    cues = ("description","item","qty","quantity","unit","unit price","rate","total","amount","price","vendor","supplier")
    best_i, best_score = -1, 0
    n = min(10, len(df))
    for i in range(n):
        row = [str(x).lower() for x in list(df.iloc[i].values)]
        score = sum(any(c in cell for c in cues) for cell in row)
        if score >= 2 and score > best_score:
            best_i, best_score = i, score
    if best_i >= 0:
        new_cols = [str(x).strip() for x in list(df.iloc[best_i].values)]
        df2 = df.iloc[best_i+1:].reset_index(drop=True).copy()
        # If columns are all empty/Unnamed, bail.
        if any(new_cols) and not all(str(c).startswith("Unnamed") for c in new_cols):
            df2.columns = new_cols
            return df2
    return df

def _read_text(name: str, data: bytes) -> str:
    low = name.lower()
    if low.endswith(".pdf") and pdfplumber:
        try:
            texts = []
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                for p in pdf.pages:
                    texts.append(p.extract_text(x_tolerance=1.5, y_tolerance=3.0) or "")
            return "\n".join(texts)
        except Exception:
            pass
    if low.endswith(".docx") and docx:
        try:
            d = docx.Document(io.BytesIO(data))
            return "\n".join(p.text for p in d.paragraphs)
        except Exception:
            pass
    enc = (chardet.detect(data) or {}).get("encoding") or "utf-8"
    try:
        return data.decode(enc, errors="ignore")
    except Exception:
        return ""

def _guess_vendor_from_preamble(df: pd.DataFrame) -> Optional[str]:
    """
    Look for a cell like 'Vendor: XYZ' / 'Supplier: XYZ' near the top of the sheet.
    """
    try:
        peek = df.head(8).astype(str).fillna("")
    except Exception:
        return None
    for row in peek.values.tolist():
        cells = [str(c).strip() for c in row]
        for i, cell in enumerate(cells):
            if re.fullmatch(r'(vendor|supplier)\s*[:\-]?', cell, re.I):
                for j in range(i+1, len(cells)):
                    cand = cells[j].strip()
                    if cand and cand.lower() != 'nan':
                        return cand[:120]
        txt = " ".join(cells)
        m = re.search(r'(vendor|supplier)\s*[:\-]\s*([A-Za-z0-9&()., \-_/]+)', txt, re.I)
        if m:
            candidate = m.group(2).strip()
            if candidate.lower() != 'nan':
                return candidate[:120]
    return None

# ----------------------------
# Budget/Actual detection
# ----------------------------
def _has_budget_actual(df: pd.DataFrame) -> bool:
    if df.empty: return False
    cols = _norm_cols(df)
    has_b = any(k in cols for k in BUDGET_KEYS)
    has_a = any(k in cols for k in ACTUAL_KEYS)
    return bool(has_b and has_a)

def extract_budget_variances(df: pd.DataFrame) -> List[Dict[str,Any]]:
    if df.empty: return []
    cols = _norm_cols(df)
    get = lambda sset: next((cols[k] for k in sset if k in cols), None)

    c_proj   = get(PROJECT_KEYS)
    c_period = get(PERIOD_KEYS)
    c_code   = get(CODE_KEYS)
    c_cat    = get(CAT_KEYS)
    c_bud    = get(BUDGET_KEYS)
    c_act    = get(ACTUAL_KEYS)

    if not (c_bud and c_act):
        return []

    out = []
    for _, r in df.iterrows():
        try:
            bud = float(str(r[c_bud]).replace(",","").strip())
            act = float(str(r[c_act]).replace(",","").strip())
        except Exception:
            continue
        row = {
            "project_id": (str(r[c_proj]).strip() if c_proj and pd.notna(r[c_proj]) else None),
            "period":     (str(r[c_period]).strip() if c_period and pd.notna(r[c_period]) else None),
            "cost_code":  (str(r[c_code]).strip() if c_code and pd.notna(r[c_code]) else None),
            "category":   (str(r[c_cat]).strip()  if c_cat  and pd.notna(r[c_cat])  else None),
            "budget_sar": bud,
            "actual_sar": act,
            "variance_sar": act - bud
        }
        out.append(row)
    return out

def extract_budget_actual_from_text(text: str) -> List[Dict[str,Any]]:
    """
    Look for paragraph blocks that contain both a 'budget' and an 'actual' amount.
    Pairs are only created when both labels and amounts exist in the same block.
    """
    if not text.strip(): return []
    blocks = re.split(r'\n\s*\n', text)
    pairs: List[Dict[str,Any]] = []
    for blk in blocks:
        blk_clean = blk.strip()
        if not blk_clean: continue
        # Find amounts by label
        mb = re.search(r'\b(budget|planned|plan)\b[^0-9\-+]*'+AMT_RE, blk_clean, re.I)
        ma = re.search(r'\b(actual|spent)\b[^0-9\-+]*'+AMT_RE, blk_clean, re.I)
        if mb and ma:
            try:
                bud = float(mb.group(2).replace(",",""))
                act = float(ma.group(2).replace(",",""))
            except Exception:
                continue
            # Derive a short description (first non-empty line)
            first_line = next((ln.strip() for ln in blk_clean.splitlines() if ln.strip()), None)
            pairs.append({
                "project_id": None,
                "period": None,
                "cost_code": None,
                "category": None,
                "description": first_line[:200] if first_line else None,
                "budget_sar": bud,
                "actual_sar": act,
                "variance_sar": act - bud
            })
    return pairs

# ----------------------------
# Procurement-like extraction (from table or text)
# ----------------------------
def _rows_from_table(df: pd.DataFrame) -> List[Dict[str,Any]]:
    # Look for vendor before trimming preamble rows
    sheet_vendor = _guess_vendor_from_preamble(df)
    # Clean up possible title/preamble rows and promote the true header
    df = _coerce_header_row(df)
    cols = _norm_cols(df)
    get = lambda sset: next((cols[k] for k in sset if k in cols), None)
    c_desc  = get(DESC_KEYS)
    c_qty   = get(QTY_KEYS)
    c_upr   = get(UPRICE_KEYS) or next((cols[k] for k in cols if ("unit" in k and "price" in k)), None)
    c_amt   = get(AMOUNT_KEYS) or next((cols[k] for k in cols if ("total" in k and "vat" not in k)), None)
    c_code  = get(CODE_KEYS)
    c_vend  = get(VENDOR_KEYS)
    out: List[Dict[str,Any]] = []
    for _, r in df.iterrows():
        item = {
            "item_code": str(r[c_code]).strip() if c_code and pd.notna(r[c_code]) else None,
            "description": str(r[c_desc]).strip() if c_desc and pd.notna(r[c_desc]) else None,
            "qty": None,
            "unit_price_sar": None,
            "amount_sar": None,
            "vendor": (str(r[c_vend]).strip() if c_vend and pd.notna(r[c_vend]) else sheet_vendor),
            "doc_date": None,
        }
        if c_qty and pd.notna(r[c_qty]):
            try: item["qty"] = float(str(r[c_qty]).replace(",",""))
            except Exception: pass
        if c_upr and pd.notna(r[c_upr]):
            try: item["unit_price_sar"] = float(str(r[c_upr]).replace(",",""))
            except Exception: pass
        if c_amt and pd.notna(r[c_amt]):
            try: item["amount_sar"] = float(str(r[c_amt]).replace(",",""))
            except Exception: pass
        # If no explicit total but qty & unit price exist, compute a safe line amount
        if item["amount_sar"] is None and (item["qty"] is not None) and (item["unit_price_sar"] is not None):
            item["amount_sar"] = round(item["qty"] * item["unit_price_sar"], 2)
        if item["unit_price_sar"] is None and (item["amount_sar"] is not None) and (item.get("qty") not in (None,0)):
            try:
                item["unit_price_sar"] = round(item["amount_sar"] / item["qty"], 2)
            except Exception:
                pass
        if not any([item["description"], item["amount_sar"], item["unit_price_sar"], item["qty"]]):
            continue
        out.append(item)
    return out

# ----------------------------
# Quote-comparison fallback
# ----------------------------
def _looks_like_line_items(df: pd.DataFrame) -> bool:
    cols = {c.strip().lower() for c in df.columns.astype(str)}
    needed = {"description","qty","unit_price_sar"}
    vendorish = {"vendor","vendor_name","supplier","supplier_name"}
    return (needed.issubset(cols)) and (len(cols.intersection(vendorish)) > 0)

def _normalize_line_items(df: pd.DataFrame) -> pd.DataFrame:
    """Return a clean frame with: description, qty, unit_price_sar, amount_sar, vendor_name."""
    cols = {c.strip().lower(): c for c in df.columns.astype(str)}
    def pick(keys, default=None):
        for k in keys:
            if k in cols: return cols[k]
        return default
    c_desc   = pick(DESC_KEYS | {"description"})
    c_qty    = pick(QTY_KEYS | {"qty"})
    c_upr    = pick(UPRICE_KEYS | {"unit_price_sar","unit rate (sar)"})
    c_amt    = pick(AMOUNT_KEYS | {"amount_sar","line total (sar)"})
    c_vendor = pick(VENDOR_KEYS | {"vendor_name","supplier"})
    out = pd.DataFrame({
        "description": df[c_desc] if c_desc in df else None,
        "qty": df[c_qty] if c_qty in df else None,
        "unit_price_sar": df[c_upr] if c_upr in df else None,
        "amount_sar": df[c_amt] if c_amt in df else None,
        "vendor_name": df[c_vendor] if c_vendor in df else None
    })
    # coerce numerics
    for k in ("qty","unit_price_sar","amount_sar"):
        if k in out:
            out[k] = pd.to_numeric(out[k], errors="coerce")
    # derive amount if missing but qty & unit present
    need_amt = out["amount_sar"].isna() & out["qty"].notna() & out["unit_price_sar"].notna()
    out.loc[need_amt, "amount_sar"] = (out.loc[need_amt, "qty"] * out.loc[need_amt, "unit_price_sar"]).round(2)
    # drop empties
    out = out.dropna(subset=["description","vendor_name","unit_price_sar"]).reset_index(drop=True)
    return out

def _quote_spread_variances(df_items: pd.DataFrame, mat_pct: float, mat_amt: float) -> List[Dict[str, Any]]:
    """
    Build 'variance-like' rows from vendor quote spreads grouped by description.
    A row is flagged if either percent spread >= mat_pct OR total spread (qty*delta) >= mat_amt.
    """
    variances: List[Dict[str,Any]] = []
    if df_items.empty: 
        return variances
    # total qty for each description across vendors (fallback: max per vendor if wildly different)
    g = df_items.groupby("description", dropna=False)
    for desc, grp in g:
        try:
            idx_min = grp["unit_price_sar"].idxmin()
            idx_max = grp["unit_price_sar"].idxmax()
        except ValueError:
            continue
        rmin = grp.loc[idx_min]
        rmax = grp.loc[idx_max]
        qty = int(np.nan_to_num(grp["qty"]).sum()) or int(np.nan_to_num(grp["qty"]).max(initial=0))
        min_u = float(rmin["unit_price_sar"])
        max_u = float(rmax["unit_price_sar"])
        if not (np.isfinite(min_u) and np.isfinite(max_u) and min_u>0):
            continue
        pct  = (max_u/min_u - 1.0) * 100.0
        d_u  = max_u - min_u
        d_tot = d_u * max(qty, 1)
        flagged = (pct >= float(mat_pct)) or (d_tot >= float(mat_amt))
        variances.append({
            "type": "quote_spread",
            "description": str(desc),
            "qty_total": qty,
            "min_vendor": str(rmin.get("vendor_name")),
            "min_unit_sar": round(min_u,2),
            "max_vendor": str(rmax.get("vendor_name")),
            "max_unit_sar": round(max_u,2),
            "unit_spread_sar": round(d_u,2),
            "spread_pct": round(pct,2),
            "total_spread_sar": round(d_tot,2),
            "flagged": bool(flagged),
        })
    # Only keep flagged rows so the UI doesn't say "none".
    return [v for v in variances if v["flagged"]]

def _vendor_totals(df_items: pd.DataFrame) -> List[Dict[str,Any]]:
    if df_items.empty or "amount_sar" not in df_items:
        return []
    tots = df_items.groupby("vendor_name", dropna=False)["amount_sar"].sum().sort_values(ascending=False)
    return [{"vendor_name": k, "amount_sar": float(v)} for k, v in tots.items()]

def _rows_from_text_items(text: str) -> Tuple[List[Dict[str,Any]], Optional[str]]:
    if not text.strip():
        return [], None
    lines = [ln for ln in text.splitlines() if ln.strip()]
    rows: List[Dict[str,Any]] = []
    doc_date = None
    for ln in lines:
        m = re.search(DATE_RE, ln, re.I)
        if m: 
            doc_date = m.group(1).strip()
            break

    curr: Dict[str,Any] = {}
    buf: List[str] = []
    def flush():
        nonlocal curr, buf
        if curr or buf:
            desc = " ".join(buf).strip() or None
            if desc and not curr.get("description"):
                curr["description"] = desc[:500]
            if any([curr.get("description"), curr.get("amount_sar"), curr.get("unit_price_sar"), curr.get("qty")]):
                rows.append(curr)
        curr, buf = {}, []

    for ln in lines:
        code = re.search(ITEM_RE, ln, re.I)
        if code:
            flush()
            curr = {"item_code": code.group(1), "description": None, "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor": None, "doc_date": doc_date}
            buf = [ln]
            mq = re.search(r'\bQty[: ]+([0-9]+)', ln, re.I) or re.search(r'\b([0-9]+)\s*(?:pcs|sets|qty)\b', ln, re.I)
            if mq: curr["qty"] = float(mq.group(1))
            mu = re.search(r'(?:Unit Price|Rate|U\.??\s*Rate).{0,10}'+AMT_RE, ln, re.I)
            if mu: 
                try: curr["unit_price_sar"] = float(mu.group(1).replace(",",""))
                except: pass
            mt = re.search(r'(?:Line\s*Total|Total|Amount).{0,10}'+AMT_RE, ln, re.I)
            if mt:
                try: curr["amount_sar"] = float(mt.group(1).replace(",",""))
                except: pass
            continue
        if curr:
            buf.append(ln)
            if curr.get("qty") is None:
                mq = re.search(r'\bQty[: ]+([0-9]+)', ln, re.I) or re.search(r'\b([0-9]+)\s*(?:pcs|sets|qty)\b', ln, re.I)
                if mq: curr["qty"] = float(mq.group(1))
            if curr.get("unit_price_sar") is None:
                mu = re.search(r'(?:Unit Price|Rate|U\.??\s*Rate).{0,12}'+AMT_RE, ln, re.I)
                if mu:
                    try: curr["unit_price_sar"] = float(mu.group(1).replace(",",""))
                    except: pass
            if curr.get("amount_sar") is None:
                mt = re.search(r'(?:Line\s*Total|Total|Amount).{0,12}'+AMT_RE, ln, re.I)
                if mt:
                    try: curr["amount_sar"] = float(mt.group(1).replace(",",""))
                    except: pass
    flush()
    for it in rows:
        if it.get("amount_sar") is None and it.get("qty") is not None and it.get("unit_price_sar") is not None:
            it["amount_sar"] = round(it["qty"] * it["unit_price_sar"], 2)
        it.setdefault("doc_date", doc_date)
    return rows, doc_date

# ----------------------------
# Insights (no speculation; computed from extracted rows)
# ----------------------------
def build_variance_insights(rows: List[Dict[str,Any]]) -> Dict[str,Any]:
    if not rows: return {"total_budget":0,"total_actual":0,"total_variance":0,"top_increases":[],"top_decreases":[]}
    # sums
    tb = sum(r.get("budget_sar",0) for r in rows if isinstance(r.get("budget_sar"), (int,float)))
    ta = sum(r.get("actual_sar",0) for r in rows if isinstance(r.get("actual_sar"), (int,float)))
    tv = ta - tb
    # sort by abs variance
    ranked = []
    for r in rows:
        b = r.get("budget_sar"); a = r.get("actual_sar")
        if isinstance(b,(int,float)) and isinstance(a,(int,float)):
            ranked.append({**r,"variance_sar":a-b})
    ranked.sort(key=lambda x: abs(x["variance_sar"]), reverse=True)
    top_inc = [x for x in ranked if x["variance_sar"]>0][:5]
    top_dec = [x for x in ranked if x["variance_sar"]<0][:5]
    return {"total_budget":tb,"total_actual":ta,"total_variance":tv,"top_increases":top_inc,"top_decreases":top_dec}

def process_single_file(name: str, data: bytes, materiality_pct: float = 5.0, materiality_amt_sar: float = 100000.0) -> Dict[str, Any]:
    """
    Existing entrypoint, now with quote-comparison fallback.
    - If we can’t detect budget/actual pairs, try to compute vendor quote spreads.
    - Returns a dict with keys:
        mode: "quote_compare" | "summary" | ...
        items / variance_items: list of rows
        vendor_totals: optional per-vendor totals
    """
    # If Excel, first try line_items sheet; else try generic table extraction.
    sheets = _read_excel_sheets(data)
    if "line_items" in sheets:
        items = _normalize_line_items(sheets["line_items"])
    else:
        # fall back to the first sheet/table we can coerce
        df0 = next(iter(sheets.values()), _read_df(name, data))
        df0 = _coerce_header_row(df0)
        items = _normalize_line_items(df0) if _looks_like_line_items(df0) else pd.DataFrame()

    # If we have recognizably vendorized items, run the quote-spread comparator
    if not items.empty and not _has_budget_actual(items):
        spreads = _quote_spread_variances(items, mat_pct=materiality_pct, mat_amt=materiality_amt_sar)
        return {
            "mode": "quote_compare",
            "variance_items": spreads,  # may be empty
            "vendor_totals": _vendor_totals(items),
            "message": None if spreads else "No items breached materiality; showing vendor totals only.",
        }
    # Otherwise, fall back to prior summary behavior using generic row extraction
    df = next(iter(sheets.values()), _read_df(name, data))
    rows = _rows_from_table(df)
    return {"mode": "summary", "items": rows}

def draft_bilingual_procurement_card(it: Dict[str,Any], file_label: str) -> Dict[str,str]:
    parts_en = []
    code = it.get("item_code"); desc = it.get("description"); qty = it.get("qty")
    upr  = it.get("unit_price_sar"); amt = it.get("amount_sar"); ven = it.get("vendor"); dt = it.get("doc_date")
    if code: parts_en.append(f"Item: {code}")
    if desc: parts_en.append(f"Description: {desc}")
    if qty is not None: parts_en.append(f"Quantity: {qty}")
    if upr is not None: parts_en.append(f"Unit price (SAR): {upr}")
    if amt is not None: parts_en.append(f"Line total (SAR): {amt}")
    if ven: parts_en.append(f"Vendor: {ven}")
    if dt:  parts_en.append(f"Document date: {dt}")
    parts_en.append(f"Evidence: {file_label}")

    parts_ar = []
    if code: parts_ar.append(f"البند: {code}")
    if desc: parts_ar.append(f"الوصف: {desc}")
    if qty is not None: parts_ar.append(f"الكمية: {qty}")
    if upr is not None: parts_ar.append(f"سعر الوحدة (ريال): {upr}")
    if amt is not None: parts_ar.append(f"الإجمالي (ريال): {amt}")
    if ven: parts_ar.append(f"المورد: {ven}")
    if dt:  parts_ar.append(f"تاريخ المستند: {dt}")
    parts_ar.append(f"الدليل: {file_label}")

    return {"en": " | ".join(parts_en), "ar": " | ".join(parts_ar)}

