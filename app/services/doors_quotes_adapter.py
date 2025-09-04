from __future__ import annotations
from typing import Any, Dict, List, Tuple
import pandas as pd
import numpy as np

REQ_LINE_ITEMS = "line_items"
REQ_VENDOR_TOTALS = "price_comparison_totals"
OPT_HIGHLIGHTS = "price_comp_highlights"

def _coerce_num(s):
    return pd.to_numeric(s, errors="coerce")

def is_doors_quotes_workbook(xls: pd.ExcelFile) -> bool:
    sns = {sn.strip().lower() for sn in xls.sheet_names}
    return (REQ_LINE_ITEMS in sns) or (REQ_VENDOR_TOTALS in sns)

def _lower_map(cols) -> Dict[str, str]:
    return {str(c).strip().lower(): str(c).strip() for c in cols}

def _read_sheet(xls: pd.ExcelFile, name: str) -> pd.DataFrame:
    return xls.parse(name, dtype=object)

def _normalize_line_items(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    low = _lower_map(df.columns)

    # Expected names from the provided workbook
    c_vendor = low.get("vendor_name") or low.get("vendor")
    c_item   = low.get("item_code") or low.get("item") or low.get("door id")
    c_desc   = low.get("description") or low.get("item description") or low.get("description of works")
    c_qty    = low.get("qty") or low.get("quantity") or low.get("no of doors") or low.get("no. of doors")
    c_u      = low.get("unit_price_sar") or low.get("unit price (sar)") or low.get("unit rate (sar)") or low.get("rate")
    c_amt    = low.get("amount_sar") or low.get("total price (sar)") or low.get("total")

    # Build items
    out = pd.DataFrame({
        "vendor_name": df[c_vendor] if c_vendor else None,
        "item_code": df[c_item] if c_item else None,
        "description": df[c_desc] if c_desc else None,
        "qty": _coerce_num(df[c_qty]) if c_qty else None,
        "unit_price_sar": _coerce_num(df[c_u]) if c_u else None,
        "amount_sar": _coerce_num(df[c_amt]) if c_amt else None,
    })
    # Backfill amount if missing
    need_amt = out["amount_sar"].isna() & out["qty"].notna() & out["unit_price_sar"].notna()
    out.loc[need_amt, "amount_sar"] = (out.loc[need_amt, "qty"] * out.loc[need_amt, "unit_price_sar"]).round(2)
    # Clean desc
    out["description"] = out["description"].astype(str).str.strip().replace({"nan": None})
    return out.dropna(subset=["vendor_name","description","unit_price_sar"]).reset_index(drop=True)

def _compute_spreads(items: pd.DataFrame, materiality_pct: float, materiality_amt_sar: float) -> List[Dict[str, Any]]:
    if items.empty:
        return []
    rows = []
    # Prefer grouping by item_code+description when available
    grp_cols = ["description"]
    if "item_code" in items.columns and items["item_code"].notna().any():
        grp_cols = ["item_code","description"]

    for key, grp in items.groupby(grp_cols, dropna=False):
        g = grp.dropna(subset=["unit_price_sar"])
        if g["vendor_name"].nunique() < 2:
            continue
        qty_total = int(_coerce_num(g["qty"]).fillna(0).sum()) or int(_coerce_num(g["qty"]).fillna(0).max())
        imin = g["unit_price_sar"].idxmin(); imax = g["unit_price_sar"].idxmax()
        rmin = g.loc[imin]; rmax = g.loc[imax]
        min_u = float(rmin["unit_price_sar"]); max_u = float(rmax["unit_price_sar"])
        if min_u <= 0: 
            continue
        spread_pct = (max_u/min_u - 1.0) * 100.0
        unit_spread = max_u - min_u
        total_spread = unit_spread * max(qty_total, 1)

        # materiality filter (0 shows everything)
        pass_pct = (materiality_pct or 0) <= 0 or spread_pct >= (materiality_pct or 0)
        pass_amt = (materiality_amt_sar or 0) <= 0 or total_spread >= (materiality_amt_sar or 0)
        if pass_pct or pass_amt:
            desc = rmin.get("description")
            item_code = rmin.get("item_code")
            rows.append({
                "item_code": item_code,
                "description": desc,
                "qty_total": qty_total,
                "min_vendor": rmin.get("vendor_name"),
                "min_unit_sar": round(min_u,2),
                "max_vendor": rmax.get("vendor_name"),
                "max_unit_sar": round(max_u,2),
                "unit_spread_sar": round(unit_spread,2),
                "spread_pct": round(spread_pct,2),
                "total_spread_sar": round(total_spread,2),
            })
    # Highest-impact first
    rows.sort(key=lambda r: (r.get("total_spread_sar",0), r.get("spread_pct",0)), reverse=True)
    return rows

def _vendor_totals_from_items(items: pd.DataFrame) -> List[Dict[str, Any]]:
    if items.empty or "amount_sar" not in items.columns:
        return []
    vt = (items.dropna(subset=["vendor_name","amount_sar"])
          .groupby("vendor_name")["amount_sar"]
          .sum()
          .sort_values(ascending=False)
          .reset_index())
    vt.columns = ["vendor_name","total_amount_sar"]
    return vt.to_dict("records")

def _vendor_totals_from_sheet(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    low = _lower_map(df.columns)
    c_vendor = low.get("vendor") or low.get("vendor_name") or next(iter(low.values()))
    c_total  = low.get("grand_total_sar") or low.get("total_sar") or low.get("amount_sar") or None
    if not c_total:
        return []
    out = df[[c_vendor, c_total]].copy()
    out.columns = ["vendor_name","total_amount_sar"]
    out["total_amount_sar"] = _coerce_num(out["total_amount_sar"])
    out = out.dropna(subset=["vendor_name","total_amount_sar"])
    return out.sort_values("total_amount_sar", ascending=False).to_dict("records")

def _highlights(df: pd.DataFrame) -> List[str]:
    if df is None or df.empty:
        return []
    msgs = []
    low = _lower_map(df.columns)
    c_item = low.get("item_code")
    c_qty  = low.get("qty") or low.get("quantity")
    c_best_u = low.get("best_unit_rate_sar")
    c_note = low.get("note")
    for _, r in df.iterrows():
        parts = []
        if c_item and pd.notna(r.get(c_item)):
            parts.append(f"{r[c_item]}")
        if c_qty and pd.notna(r.get(c_qty)):
            parts.append(f"qty {r[c_qty]}")
        if c_best_u and pd.notna(r.get(c_best_u)):
            parts.append(f"best unit {r[c_best_u]} SAR")
        if c_note and pd.notna(r.get(c_note)):
            parts.append(str(r[c_note]))
        if parts:
            msgs.append(" – ".join(parts))
    return msgs[:20]

def adapt(xls: pd.ExcelFile, materiality_pct: float, materiality_amt_sar: float) -> Dict[str, Any]:
    """
    Parse the 'doors quotes' workbook and produce the standard quote_compare payload.
    """
    # Sheets
    li = None; vt_sheet = None; hl = None
    for sn in xls.sheet_names:
        lsn = sn.strip().lower()
        if lsn == REQ_LINE_ITEMS:
            li = _read_sheet(xls, sn)
        elif lsn == REQ_VENDOR_TOTALS:
            vt_sheet = _read_sheet(xls, sn)
        elif lsn == OPT_HIGHLIGHTS:
            hl = _read_sheet(xls, sn)

    items = _normalize_line_items(li) if li is not None else pd.DataFrame()
    spreads = _compute_spreads(items, materiality_pct, materiality_amt_sar)

    # Vendor totals: prefer explicit totals sheet; fallback to items
    vendor_totals = _vendor_totals_from_sheet(vt_sheet) if vt_sheet is not None else _vendor_totals_from_items(items)

    message = None
    if not spreads and not items.empty:
        message = "No items breached filters; showing vendor totals."
    if items.empty and vendor_totals:
        message = "No line items detected; vendor totals shown from summary sheet."

    # Optional highlights
    insights = {"highlights": _highlights(hl)} if hl is not None else {}

    return {
        "mode": "quote_compare",
        "variance_items": spreads,       # may be empty
        "vendor_totals": vendor_totals,  # may be empty
        "items_rowcount": int(items.shape[0]),
        "message": message,
        "insights": insights
    }
