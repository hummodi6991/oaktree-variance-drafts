from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd
import math
from collections import Counter, defaultdict

# ---------- Helpers ----------
NUMERIC_NA_REPR = None

def _norm_col(c: Any) -> str:
    return str(c).strip()

def _lowset(cols) -> Dict[str, str]:
    return {str(c).strip().lower(): str(c).strip() for c in cols}

def _is_date_series(s: pd.Series) -> bool:
    try:
        pd.to_datetime(s, errors="raise")
        return True
    except Exception:
        return False

def _numeric_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

def _categorical_cols(df: pd.DataFrame) -> List[str]:
    cats = []
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            continue
        # exclude obviously unique-like columns (e.g., long text)
        nunique = df[c].nunique(dropna=True)
        if 1 <= nunique <= max(50, int(len(df) * 0.1)):
            cats.append(c)
    return cats

def _pick_amount_col(cols_low: Dict[str, str]) -> str | None:
    for key in (
        "amount","amount_sar","total","total_sar","line total","line_total","net amount",
        "value","total price","total price (sar)","extended amount","subtotal","grand total"
    ):
        if key in cols_low:
            return cols_low[key]
    return None

def _pick_qty_col(cols_low: Dict[str, str]) -> str | None:
    for key in ("qty","quantity","qty (nos)","nos","units","no.","no of doors","no of units"):
        if key in cols_low:
            return cols_low[key]
    return None

def _pick_unit_price_col(cols_low: Dict[str, str]) -> str | None:
    for key in ("unit price","unit_price","unit rate","unit rate (sar)","unit price (sar)","rate","price per unit","price/unit","u rate","u.rate"):
        if key in cols_low:
            return cols_low[key]
    return None

def _pick_vendor_col(cols_low: Dict[str, str]) -> str | None:
    for key in ("vendor","vendor name","vendor_name","supplier","supplier name","company","quoted by","bidder","vendor/supplier"):
        if key in cols_low:
            return cols_low[key]
    return None

def _pick_desc_col(cols_low: Dict[str, str]) -> str | None:
    for key in ("description","description of works","item description","work description","scope","item"):
        if key in cols_low:
            return cols_low[key]
    return None

# ---------- Public API ----------

def generate_insights_for_workbook(sheets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Summarize arbitrary tabular workbooks that DO NOT contain budget/actual pairs.
    Produces a compact 'insights' payload consumable by the UI.
    """
    profile: Dict[str, Any] = {"sheets": []}
    tables: Dict[str, Any] = {}
    highlights: List[str] = []
    cards: List[Dict[str, Any]] = []

    grand_amount_total: float = 0.0
    vendor_totals_accum: Counter[str] = Counter()
    spend_by_desc_accum: Counter[str] = Counter()
    spread_rows: List[Dict[str, Any]] = []

    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            profile["sheets"].append({"sheet": sheet_name, "rows": 0, "cols": 0})
            continue

        df = df.copy()
        df.columns = [_norm_col(c) for c in df.columns]
        profile["sheets"].append({"sheet": sheet_name, "rows": int(df.shape[0]), "cols": int(df.shape[1])})

        # Column profiling
        cols = []
        for c in df.columns:
            s = df[c]
            dtype = str(s.dtype)
            missing = float(s.isna().mean()) if len(s) else 0.0
            is_date = _is_date_series(s) if dtype == "object" else False
            cols.append({"column": c, "dtype": dtype, "missing_rate": round(missing, 4), "date_like": bool(is_date)})
        tables[f"{sheet_name}::columns_profile"] = cols

        # Numeric summary
        num_cols = _numeric_cols(df)
        if num_cols:
            summary_rows = []
            for c in num_cols:
                s = pd.to_numeric(df[c], errors="coerce")
                s = s.dropna()
                if s.empty: 
                    continue
                summary_rows.append({
                    "column": c,
                    "count": int(s.count()),
                    "sum": float(s.sum()),
                    "mean": float(s.mean()),
                    "median": float(s.median()),
                    "min": float(s.min()),
                    "max": float(s.max()),
                })
            if summary_rows:
                tables[f"{sheet_name}::numeric_summary"] = summary_rows

        # Categorical summary
        cat_cols = _categorical_cols(df)
        for c in cat_cols:
            vc = (df[c].astype(str).replace({"nan": None}).dropna()).value_counts().head(10)
            tables[f"{sheet_name}::top_values::{c}"] = [{"value": str(k), "count": int(v)} for k, v in vc.items()]

        # Domain-aware cost insights
        low = _lowset(df.columns)
        c_amount = _pick_amount_col(low)
        _ = _pick_qty_col(low)
        c_uprice = _pick_unit_price_col(low)
        c_vendor = _pick_vendor_col(low)
        c_desc = _pick_desc_col(low)

        if c_amount:
            amt = pd.to_numeric(df[c_amount], errors="coerce")
            total_amt = float(amt.dropna().sum())
            grand_amount_total += total_amt
            cards.append({"sheet": sheet_name, "title": "Sheet total amount", "value_sar": round(total_amt, 2)})

        if c_vendor and c_amount:
            grp = (df[[c_vendor, c_amount]]
                   .assign(_amt=pd.to_numeric(df[c_amount], errors="coerce"))
                   .dropna(subset=["_amt"]))
            if not grp.empty:
                vt = grp.groupby(c_vendor)["_amt"].sum().sort_values(ascending=False).head(10)
                vendor_totals_accum.update({str(k): float(v) for k, v in vt.items()})
                tables[f"{sheet_name}::vendor_totals"] = [{"vendor": str(k), "total_sar": float(v)} for k, v in vt.items()]

        if c_desc and c_amount:
            ds = (df[[c_desc, c_amount]]
                  .assign(_amt=pd.to_numeric(df[c_amount], errors="coerce"))
                  .dropna(subset=["_amt"]))
            if not ds.empty:
                top_items = ds.groupby(c_desc)["_amt"].sum().sort_values(ascending=False).head(10)
                spend_by_desc_accum.update({str(k): float(v) for k, v in top_items.items()})
                tables[f"{sheet_name}::top_items_by_spend"] = [{"description": str(k), "total_sar": float(v)} for k, v in top_items.items()]

        # Vendor spread insights (if we have vendor + unit price + description)
        if c_vendor and c_uprice and c_desc:
            tmp = df[[c_vendor, c_uprice, c_desc]].copy()
            tmp["_u"] = pd.to_numeric(tmp[c_uprice], errors="coerce")
            tmp = tmp.dropna(subset=["_u", c_vendor, c_desc])
            if not tmp.empty:
                for desc, grp in tmp.groupby(c_desc):
                    if grp[c_vendor].nunique() < 2:
                        continue
                    umin = grp.loc[grp["_u"].idxmin()]
                    umax = grp.loc[grp["_u"].idxmax()]
                    min_u = float(umin["_u"])
                    max_u = float(umax["_u"])
                    if min_u <= 0: 
                        continue
                    spread_pct = (max_u/min_u - 1.0) * 100.0
                    spread_rows.append({
                        "description": str(desc),
                        "min_vendor": str(umin[c_vendor]),
                        "min_unit_sar": round(min_u, 2),
                        "max_vendor": str(umax[c_vendor]),
                        "max_unit_sar": round(max_u, 2),
                        "unit_spread_sar": round(max_u - min_u, 2),
                        "spread_pct": round(spread_pct, 2),
                    })

    # Workbook-level aggregates
    if vendor_totals_accum:
        vt_sorted = sorted(vendor_totals_accum.items(), key=lambda kv: kv[1], reverse=True)
        tables["workbook::vendor_totals"] = [{"vendor": v, "total_sar": round(t, 2)} for v, t in vt_sorted[:20]]
        if vt_sorted:
            v, t = vt_sorted[0]
            highlights.append(f"Top vendor by spend: {v} (≈ {round(t,2)} SAR).")

    if spend_by_desc_accum:
        items_sorted = sorted(spend_by_desc_accum.items(), key=lambda kv: kv[1], reverse=True)[:5]
        top_desc = ", ".join([f"{d} (≈ {round(v,2)} SAR)" for d, v in items_sorted])
        highlights.append(f"Largest cost drivers: {top_desc}.")

    if spread_rows:
        spread_rows = sorted(spread_rows, key=lambda r: (r.get("unit_spread_sar",0), r.get("spread_pct",0)), reverse=True)[:20]
        tables["workbook::vendor_spreads"] = spread_rows
        max_spread = spread_rows[0]
        highlights.append(
            f"Bid spread detected for '{max_spread['description']}': "
            f"{max_spread['min_vendor']} {max_spread['min_unit_sar']} vs "
            f"{max_spread['max_vendor']} {max_spread['max_unit_sar']} SAR "
            f"({max_spread['spread_pct']}% Δ)."
        )

    if grand_amount_total > 0:
        cards.insert(0, {"title": "Workbook total amount", "value_sar": round(grand_amount_total, 2)})

    return {
        "profile": profile,
        "tables": tables,      # dict of named tables
        "cards": cards,        # small KPI cards
        "highlights": highlights
    }


DEFAULT_BASKET: Dict[str, int] = {"D01": 9, "D02": 18, "D03": 27, "D04": 12}


def compute_procurement_insights(
    items: List[Dict[str, Any]],
    basket: Optional[Dict[str, int]] = None,
    vat_rate: float = 0.15,
) -> Dict[str, Any]:
    """Simple rollups for procurement line items.

    Returns totals per vendor and top lines by amount to help the UI render
    basic summaries when no budget/actual variance is present.
    If ``basket`` is provided, compute per-vendor pricing for the requested quantities.
    """
    vendor_totals: Dict[str, float] = defaultdict(float)
    top_lines: List[Dict[str, Any]] = []
    basket_totals: Dict[str, float] = defaultdict(float)

    for it in items:
        amt = it.get("amount_sar")
        try:
            amt = float(amt) if amt is not None else None
        except Exception:
            amt = None
        vendor = it.get("vendor_name") or it.get("vendor")
        if amt is not None and vendor:
            vendor_totals[str(vendor)] += amt
        if amt is not None:
            top_lines.append({**it, "amount_sar": amt})
        if basket and vendor and it.get("item_code") in basket and it.get("unit_price_sar") is not None:
            try:
                unit_p = float(it.get("unit_price_sar"))
                qty_req = float(basket[it["item_code"]])
                basket_totals[str(vendor)] += unit_p * qty_req
            except Exception:
                pass

    top_lines = sorted(top_lines, key=lambda r: r.get("amount_sar", 0), reverse=True)[:10]
    vendor_totals_sorted = sorted(vendor_totals.items(), key=lambda kv: kv[1], reverse=True)

    analysis: Dict[str, Any] = {
        "totals_per_vendor": [{"vendor": v, "total_sar": round(t, 2)} for v, t in vendor_totals_sorted],
        "top_lines_by_amount": top_lines,
    }

    if basket_totals:
        basket_rows = []
        for v, net in basket_totals.items():
            vat = net * vat_rate
            basket_rows.append({
                "vendor": v,
                "net_amount_sar": round(net, 2),
                "vat_amount_sar": round(vat, 2),
                "tco_sar": round(net + vat, 2),
            })
        analysis["vendor_basket_totals"] = basket_rows

        # Line-level best unit prices
        df = pd.DataFrame(items)
        df = df.dropna(subset=["item_code", "vendor_name", "unit_price_sar"])
        if not df.empty:
            bench: List[Dict[str, Any]] = []
            for code, grp in df.groupby("item_code"):
                try:
                    min_u = float(grp["unit_price_sar"].min())
                    max_u = float(grp["unit_price_sar"].max())
                    spread = max_u - min_u
                    spread_pct = (spread / min_u * 100.0) if min_u > 0 else None
                    bench.append({
                        "item_code": code,
                        "min_unit_price_sar": round(min_u, 2),
                        "max_unit_price_sar": round(max_u, 2),
                        "unit_price_spread_sar": round(spread, 2),
                        "unit_price_spread_pct": round(spread_pct, 2) if spread_pct is not None else None,
                    })
                except Exception:
                    continue
            if bench:
                analysis["line_benchmarks"] = bench

    return analysis


def summarize_procurement_lines(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return a tiny summary for procurement lines.

    This helper avoids returning the raw item cards in API responses when no
    budget/actual pairs are detected. It computes lightweight highlights such as
    total line count, distinct vendor count and aggregate amount if available.
    """
    vendors: set[str] = set()
    total = 0.0
    for it in items or []:
        v = it.get("vendor_name") or it.get("vendor")
        if v:
            vendors.add(str(v))
        try:
            amt = float(it.get("amount_sar")) if it.get("amount_sar") is not None else None
        except Exception:
            amt = None
        if amt is not None:
            total += amt
    highlights = [f"{len(items)} line(s) detected."]
    if vendors:
        highlights.append(f"{len(vendors)} vendor(s) present.")
    if total:
        highlights.append(f"Total amount ≈ {round(total, 2):,} SAR.")
    return {"highlights": highlights}


# --- Variance insights for Budget vs Actual ---
def compute_variance_insights(variance_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Rollups for Budget vs Actual variance rows produced by the single-file track.
    Each item is expected to contain keys: label, budget_sar, actual_sar, variance_sar.
    """
    rows: List[Dict[str, Any]] = []
    tot_budget = 0.0
    tot_actual = 0.0
    for r in variance_items:
        try:
            b = float(r.get("budget_sar")) if r.get("budget_sar") is not None else None
            a = float(r.get("actual_sar")) if r.get("actual_sar") is not None else None
            v = float(r.get("variance_sar")) if r.get("variance_sar") is not None else (
                a - b if a is not None and b is not None else None
            )
        except Exception:
            b = a = v = None
        rows.append({**r, "budget_sar": b, "actual_sar": a, "variance_sar": v})
        if b is not None:
            tot_budget += b
        if a is not None:
            tot_actual += a

    tot_variance: Optional[float] = (
        (tot_actual - tot_budget)
        if (not math.isnan(tot_actual) and not math.isnan(tot_budget))
        else None
    )
    over = [r for r in rows if r.get("variance_sar") is not None and r.get("variance_sar") > 0]
    under = [r for r in rows if r.get("variance_sar") is not None and r.get("variance_sar") < 0]
    top_overruns = sorted(over, key=lambda r: r["variance_sar"], reverse=True)[:10]
    top_underruns = sorted(under, key=lambda r: r["variance_sar"])[:10]

    return {
        "totals": {
            "budget_sar": round(tot_budget, 2),
            "actual_sar": round(tot_actual, 2),
            "variance_sar": round(tot_variance, 2) if tot_variance is not None else None,
        },
        "top_overruns": top_overruns,
        "top_underruns": top_underruns,
    }
