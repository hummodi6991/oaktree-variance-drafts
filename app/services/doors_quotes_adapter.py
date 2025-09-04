from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import re
import pandas as pd
import numpy as np

# Canonical sheet name hints we may see
REQ_LINE_ITEMS_HINTS = {
    "line_items","lines","items","quotation","quote","pricing","price comparison","price comparison - items"
}
REQ_VENDOR_TOTALS_HINTS = {
    "price_comparison_totals","totals","vendor totals","summary","comparison summary"
}
OPT_HIGHLIGHTS_HINTS = {"price_comp_highlights","highlights","notes"}

# --- Utilities ---------------------------------------------------------------
ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def _strip_to_number(s: Any) -> Optional[float]:
    """Parse numbers like '1,234.56 SAR', '(1,000)', '١٢٣٫٤٥', 'ر.س 5,000'."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return np.nan
    if isinstance(s, (int, float, np.integer, np.floating)):
        return float(s)
    txt = str(s).strip()
    if not txt:
        return np.nan
    # normalize arabic digits and separators
    txt = txt.translate(ARABIC_DIGITS)
    txt = txt.replace("٬","" ).replace("٫",".")  # Arabic thousands/decimal
    neg = False
    if txt.startswith("(") and txt.endswith(")"):
        neg = True
        txt = txt[1:-1]
    # remove currency & non numeric
    txt = re.sub(r"[^\d\.\-+]", "", txt)
    if txt.count(".") > 1:
        # if many dots, drop all but last
        head, _, tail = txt.rpartition(".")
        head = re.sub(r"\.", "", head)
        txt = head + "." + tail
    if txt in ("", "-", "+", ".", "-.", "+."):
        return np.nan
    try:
        val = float(txt)
        return -val if neg else val
    except Exception:
        return np.nan

def _coerce_num_series(s: pd.Series) -> pd.Series:
    return s.apply(_strip_to_number)

def _lower_map(cols) -> Dict[str, str]:
    return {str(c).strip().lower(): str(c).strip() for c in cols}

def _read_sheet(xls: pd.ExcelFile, name: str) -> pd.DataFrame:
    # read without header so we can detect table structure ourselves
    return xls.parse(name, header=None, dtype=object)

def _sheet_name_key(sn: str) -> str:
    return re.sub(r"\s+", " ", sn.strip().lower())

def _looks_like_header_row(row_vals: List[str]) -> bool:
    cues = ("description","item","qty","quantity","unit","unit price","unit rate","rate","total","amount",
            "vendor","supplier","price","sar","unit price (sar)","unit rate (sar)","door")
    found = 0
    for cell in row_vals:
        c = str(cell).strip().lower()
        if not c:
            continue
        if any(k in c for k in cues):
            found += 1
    return found >= 2

def _detect_table(df: pd.DataFrame) -> pd.DataFrame:
    """Try to find the header row within the first ~10 rows and set columns accordingly."""
    if df is None or df.empty:
        return df
    for i in range(min(10, len(df))):
        row = [str(x) for x in list(df.iloc[i].values)]
        if _looks_like_header_row(row):
            cols = [str(x).strip() for x in row]
            body = df.iloc[i+1:].reset_index(drop=True).copy()
            if any(cols) and not all(str(c).startswith("Unnamed") for c in cols):
                body.columns = cols
                return body
    # fallback: assume first row is header
    return df

# Column synonyms (EN + AR where common)
DESC_KEYS   = {"description","description of works","item description","work description","scope","الوصف"}
ITEM_KEYS   = {"item","item no","item code","item_code","door id","الكود"}
QTY_KEYS    = {"qty","quantity","qty (nos)","nos","units","no.","no of doors","no. of doors","الكمية"}
UPRICE_KEYS = {"unit_price_sar","unit price (sar)","unit rate (sar)","unit price","unit rate","rate","price per unit","price/unit","u rate","سعر الوحدة"}
AMT_KEYS    = {"amount_sar","total price (sar)","total","line total","extended amount","net amount","value","الإجمالي"}
VENDOR_KEYS = {"vendor_name","vendor","supplier","supplier name","company","quoted by","bidder","vendor/supplier","المورد","الشركة"}

def _pick(low: Dict[str,str], keys: set[str]) -> Optional[str]:
    for k in keys:
        if k in low:
            return low[k]
    return None

def _extract_vendor_preamble(df: pd.DataFrame) -> Optional[str]:
    """Look for a 'Vendor:' or Arabic equivalent in first 10 rows/columns."""
    if df is None or df.empty:
        return None
    for i in range(min(10, len(df))):
        for j in range(min(10, df.shape[1])):
            cell = str(df.iat[i, j]).strip()
            cell_l = cell.lower()
            if any(tag in cell_l for tag in ("vendor","supplier","company","المورد","الشركة")):
                # try right neighbor or after ':' 
                right = str(df.iat[i, j+1]) if j+1 < df.shape[1] else ""
                after = cell.split(":",1)[1].strip() if ":" in cell else ""
                cand = after or right
                cand = cand.strip()
                if cand and cand.lower() not in ("nan", "none"):
                    return cand
    return None

def is_doors_quotes_workbook(xls: pd.ExcelFile, file_name: Optional[str] = None) -> bool:
    sns = {_sheet_name_key(sn) for sn in xls.sheet_names}
    if any(h in sns for h in REQ_LINE_ITEMS_HINTS | REQ_VENDOR_TOTALS_HINTS):
        return True
    # Heuristic: any sheet that contains a row with typical header cues
    try:
        for sn in xls.sheet_names:
            df = _read_sheet(xls, sn)
            df2 = _detect_table(df)
            if _looks_like_header_row([str(x) for x in list(df2.columns)]):
                low = _lower_map(df2.columns)
                if (_pick(low, DESC_KEYS) and (_pick(low, UPRICE_KEYS) or _pick(low, AMT_KEYS))):
                    return True
    except Exception:
        pass
    # filename hint
    if file_name and "doors_quotes" in file_name.lower():
        return True
    return False

def _normalize_line_items(df: pd.DataFrame, vendor_hint: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["vendor_name","item_code","description","qty","unit_price_sar","amount_sar"])
    df = _detect_table(df.copy())
    df.columns = [str(c).strip() for c in df.columns]
    low = _lower_map(df.columns)
    c_vendor = _pick(low, VENDOR_KEYS)
    c_item   = _pick(low, ITEM_KEYS)
    c_desc   = _pick(low, DESC_KEYS)
    c_qty    = _pick(low, QTY_KEYS)
    c_u      = _pick(low, UPRICE_KEYS)
    c_amt    = _pick(low, AMT_KEYS)

    # If vendor column missing, try preamble or provided hint
    v_preamble = _extract_vendor_preamble(df) or vendor_hint
    vendor_col = df[c_vendor] if c_vendor else (pd.Series([v_preamble]*len(df)) if v_preamble else None)

    out = pd.DataFrame({
        "vendor_name": vendor_col,
        "item_code": df[c_item] if c_item else None,
        "description": df[c_desc] if c_desc else None,
        "qty": _coerce_num_series(df[c_qty]) if c_qty else None,
        "unit_price_sar": _coerce_num_series(df[c_u]) if c_u else None,
        "amount_sar": _coerce_num_series(df[c_amt]) if c_amt else None,
    })
    # Backfill amount if missing
    if "amount_sar" in out.columns:
        need_amt = out["amount_sar"].isna() & out["qty"].notna() & out["unit_price_sar"].notna()
        qty = pd.to_numeric(out.loc[need_amt, "qty"], errors="coerce")
        unit = pd.to_numeric(out.loc[need_amt, "unit_price_sar"], errors="coerce")
        out.loc[need_amt, "amount_sar"] = (qty * unit).round(2)
    out["description"] = out["description"].astype(str).str.strip().replace({"nan": None})
    return out.dropna(subset=["description"]).reset_index(drop=True)

def _collect_items_from_all_sheets(xls: pd.ExcelFile) -> pd.DataFrame:
    """Support both single consolidated table and per-vendor sheets."""
    frames: List[pd.DataFrame] = []
    for sn in xls.sheet_names:
        key = _sheet_name_key(sn)
        df = _read_sheet(xls, sn)
        # If sheet name looks like a totals/highlights page, skip for items
        if any(h in key for h in REQ_VENDOR_TOTALS_HINTS | OPT_HIGHLIGHTS_HINTS):
            continue
        # Use sheet name as vendor hint if it looks like a company name
        vendor_hint = None if any(w in key for w in ("line","item","quote","pricing","prices","comparison","summary","total","sheet")) else sn
        norm = _normalize_line_items(df, vendor_hint=vendor_hint)
        # Drop sheets that don't have unit price OR amount (not a pricing table)
        if "unit_price_sar" in norm and norm["unit_price_sar"].notna().any():
            frames.append(norm)
        elif "amount_sar" in norm and norm["amount_sar"].notna().any():
            frames.append(norm)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["vendor_name","item_code","description","qty","unit_price_sar","amount_sar"])

def _compute_spreads(items: pd.DataFrame, materiality_pct: float, materiality_amt_sar: float) -> List[Dict[str, Any]]:
    if items.empty or "unit_price_sar" not in items.columns:
        return []
    rows: List[Dict[str, Any]] = []
    grp_cols = ["description"]
    if "item_code" in items.columns and items["item_code"].notna().any():
        grp_cols = ["item_code","description"]
    for _, grp in items.groupby(grp_cols, dropna=False):
        g = grp.dropna(subset=["unit_price_sar"])
        if g.empty or g["vendor_name"].nunique(dropna=True) < 2:
            continue
        qty_total = int(_coerce_num_series(g.get("qty", pd.Series([]))).fillna(0).sum()) or int(_coerce_num_series(g.get("qty", pd.Series([]))).fillna(0).max())
        imin = g["unit_price_sar"].idxmin(); imax = g["unit_price_sar"].idxmax()
        rmin = g.loc[imin]; rmax = g.loc[imax]
        min_u = float(rmin["unit_price_sar"]); max_u = float(rmax["unit_price_sar"])
        if min_u <= 0:
            continue
        spread_pct = (max_u/min_u - 1.0) * 100.0
        unit_spread = max_u - min_u
        total_spread = unit_spread * max(qty_total, 1)
        pass_pct = (materiality_pct or 0) <= 0 or spread_pct >= (materiality_pct or 0)
        pass_amt = (materiality_amt_sar or 0) <= 0 or total_spread >= (materiality_amt_sar or 0)
        if pass_pct or pass_amt:
            rows.append({
                "item_code": rmin.get("item_code"),
                "description": rmin.get("description"),
                "qty_total": qty_total,
                "min_vendor": rmin.get("vendor_name"),
                "min_unit_sar": round(min_u,2),
                "max_vendor": rmax.get("vendor_name"),
                "max_unit_sar": round(max_u,2),
                "unit_spread_sar": round(unit_spread,2),
                "spread_pct": round(spread_pct,2),
                "total_spread_sar": round(total_spread,2),
            })
    rows.sort(key=lambda r: (r.get("total_spread_sar",0), r.get("spread_pct",0)), reverse=True)
    return rows

def _vendor_totals_from_items(items: pd.DataFrame) -> List[Dict[str, Any]]:
    if items.empty:
        return []
    amt_col = "amount_sar" if "amount_sar" in items.columns else None
    if not amt_col:
        # derive amount from qty * unit if possible
        if "qty" in items.columns and "unit_price_sar" in items.columns:
            tmp = items.copy()
            tmp["_amt"] = _coerce_num_series(tmp["qty"]) * _coerce_num_series(tmp["unit_price_sar"])
            amt_col = "_amt"
            items = tmp
        else:
            return []
    vt = (items.dropna(subset=["vendor_name", amt_col])
                .groupby("vendor_name")[amt_col]
                .sum()
                .sort_values(ascending=False)
                .reset_index())
    return vt.rename(columns={amt_col:"total_amount_sar"}).to_dict("records")

def _vendor_totals_from_sheet(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df = _detect_table(df.copy())
    low = _lower_map(df.columns)
    c_vendor = _pick(low, VENDOR_KEYS) or next(iter(low.values()))
    c_total  = _pick(low, {"grand_total_sar","total_sar","amount_sar","total","الإجمالي"})
    if not c_total:
        return []
    out = df[[c_vendor, c_total]].copy()
    out.columns = ["vendor_name","total_amount_sar"]
    out["total_amount_sar"] = _coerce_num_series(out["total_amount_sar"])
    out = out.dropna(subset=["vendor_name","total_amount_sar"])
    return out.sort_values("total_amount_sar", ascending=False).to_dict("records")

def _find_sheet_by_hints(xls: pd.ExcelFile, hints: set[str]) -> Optional[str]:
    sns = {_sheet_name_key(sn): sn for sn in xls.sheet_names}
    for h in hints:
        if h in sns:
            return sns[h]
    # partial match
    for key, orig in sns.items():
        if any(h in key for h in hints):
            return orig
    return None

def _highlights(df: pd.DataFrame) -> List[str]:
    if df is None or df.empty:
        return []
    df = _detect_table(df.copy())
    msgs: List[str] = []
    for _, r in df.head(50).iterrows():
        parts = [str(v) for v in r.values if pd.notna(v)]
        if parts:
            msgs.append(" – ".join(parts[:4]))
    return msgs[:20]

def adapt(xls: pd.ExcelFile, materiality_pct: float, materiality_amt_sar: float) -> Dict[str, Any]:
    """
    Parse 'doors_quotes_complete' and similar workbooks and produce the standard quote_compare payload.
    Works for:
      • single consolidated line-items sheet
      • per-vendor sheets (no vendor column)
      • optional totals/highlights sheets
    """
    # Try to locate canonical sheets; if not found, we’ll harvest items from all sheets.
    li_name = _find_sheet_by_hints(xls, REQ_LINE_ITEMS_HINTS)
    totals_name = _find_sheet_by_hints(xls, REQ_VENDOR_TOTALS_HINTS)
    highlights_name = _find_sheet_by_hints(xls, OPT_HIGHLIGHTS_HINTS)

    items = pd.DataFrame()
    if li_name:
        items = _normalize_line_items(_read_sheet(xls, li_name))
    else:
        items = _collect_items_from_all_sheets(xls)

    spreads = _compute_spreads(items, materiality_pct, materiality_amt_sar)

    vendor_totals: List[Dict[str, Any]] = []
    if totals_name:
        vendor_totals = _vendor_totals_from_sheet(_read_sheet(xls, totals_name))
    if not vendor_totals:
        vendor_totals = _vendor_totals_from_items(items)

    message = None
    if not spreads and not items.empty:
        message = "No items breached filters; showing vendor totals."
    if items.empty and vendor_totals:
        message = "No line items detected; vendor totals shown from summary sheet."

    insights = {"highlights": _highlights(_read_sheet(xls, highlights_name))} if highlights_name else {}

    payload: Dict[str, Any] = {
        "mode": "quote_compare",
        "variance_items": spreads,
        "vendor_totals": vendor_totals,
        "items_rowcount": int(items.shape[0]),
        "message": message,
        "insights": insights,
    }

    # --- Fallback variance detector ---------------------------------------
    if not payload.get("variance_items"):
        try:
            sheet_names = [n for n in xls.sheet_names if "line" in n.lower() or "item" in n.lower()] or xls.sheet_names
            df = xls.parse(sheet_names[0]).copy()
            df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
            vendor_col = next((c for c in ["vendor_name","vendor","supplier","company"] if c in df.columns), None)
            item_col   = next((c for c in ["item_code","item","description","product","model"] if c in df.columns), None)
            unit_col   = next((c for c in ["unit_price_sar","unit_price","unit_rate","price","unit_cost","rate"] if c in df.columns), None)
            if vendor_col and item_col and unit_col:
                df[unit_col] = df[unit_col].apply(_strip_to_number)
                base = df[[item_col, vendor_col, unit_col]].dropna()
                vc = base.groupby(item_col)[vendor_col].nunique()
                multi = vc[vc >= 2].index
                base = base[base[item_col].isin(multi)]
                if len(base):
                    g = base.groupby(item_col)[unit_col]
                    spread = pd.DataFrame({
                        "item_code": g.apply(lambda s: s.name),
                        "min_unit_price_sar": g.min(),
                        "max_unit_price_sar": g.max(),
                    }).reset_index(drop=True)
                    spread["unit_price_spread_sar"] = spread["max_unit_price_sar"] - spread["min_unit_price_sar"]
                    spread["unit_price_spread_pct"] = np.where(
                        spread["min_unit_price_sar"] > 0,
                        (spread["unit_price_spread_sar"] / spread["min_unit_price_sar"]) * 100,
                        np.nan,
                    )
                    payload["variance_items"] = (
                        spread.sort_values("unit_price_spread_sar", ascending=False).to_dict("records")
                    )
        except Exception as e:  # pragma: no cover - defensive
            payload.setdefault("debug_notes", []).append(f"fallback_variance_failed: {e}")

    # --- Attach report markdown -------------------------------------------
    try:
        from app.services.singlefile import _build_report_markdown_for_quote_compare

        payload["report_markdown"] = _build_report_markdown_for_quote_compare(
            payload, getattr(xls, "io", "workbook")
        )
    except Exception:  # pragma: no cover - defensive
        pass

    return payload

