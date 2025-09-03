from typing import Dict, Any, List, Optional
import io
import re
import pandas as pd

# --- Helpers: tolerant column detection & coercion ---

_BUDGET_SYNONYMS = [
    "budget", "budget_sar", "planned", "plan", "estimate", "estimated", "boq_budget",
    "original_budget", "revised_budget"
]
_ACTUAL_SYNONYMS = [
    "actual", "actual_sar", "spent", "cost", "paid", "invoice_total", "ytd_actual",
    "accrual", "expended"
]
_PERIOD_SYNONYMS = ["period", "month", "posting_period", "date"]
_COST_CODE_SYNONYMS = ["cost_code", "code", "account", "line_code"]
_CATEGORY_SYNONYMS = ["category", "cost_category", "trade"]
_PROJECT_SYNONYMS = ["project_id", "project", "project_name"]

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower())

def _find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    norm_map = {c: _norm(c) for c in cols}
    for cand in candidates:
        c_norm = _norm(cand)
        for col, ncol in norm_map.items():
            if ncol == c_norm or ncol.endswith("_" + c_norm) or c_norm in ncol:
                return col
    return None

def _coerce_number(series: pd.Series) -> pd.Series:
    # Remove currency words/symbols and thousands separators
    s = series.astype(str).str.replace(r"[^\d\-\.\,]", "", regex=True)
    # If commas abound and dots are few, treat comma as thousands sep
    s = s.str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def _first_nonempty(series: pd.Series) -> Optional[str]:
    for v in series.astype(str):
        vv = v.strip()
        if vv:
            return vv
    return None

def detect_and_compute_variances(df: pd.DataFrame) -> Dict[str, Any]:
    cols = list(df.columns)
    bcol = _find_col(cols, _BUDGET_SYNONYMS)
    acol = _find_col(cols, _ACTUAL_SYNONYMS)
    if not bcol or not acol:
        return {
            "mode": "summary_only",
            "reason": "budget_or_actual_missing",
            "columns_seen": cols,
        }

    # Optional grouping columns
    pcol = _find_col(cols, _PROJECT_SYNONYMS)
    percol = _find_col(cols, _PERIOD_SYNONYMS)
    codecol = _find_col(cols, _COST_CODE_SYNONYMS)
    catcol = _find_col(cols, _CATEGORY_SYNONYMS)

    work = df.copy()
    work["_budget"] = _coerce_number(work[bcol])
    work["_actual"] = _coerce_number(work[acol])
    work["_variance"] = work["_actual"] - work["_budget"]
    work["_variance_pct"] = work.apply(
        lambda r: (r["_variance"] / r["_budget"] * 100.0) if r["_budget"] else 0.0,
        axis=1
    )

    group_cols = [c for c in [pcol, percol, codecol, catcol] if c]
    if group_cols:
        agg = (work
               .groupby(group_cols, dropna=False)[["_budget", "_actual", "_variance"]]
               .sum()
               .reset_index())
        agg["_variance_pct"] = agg.apply(
            lambda r: (r["_variance"] / r["_budget"] * 100.0) if r["_budget"] else 0.0,
            axis=1
        )
        rows = []
        for _, r in agg.iterrows():
            rows.append({
                "project_id": (r.get(pcol) if pcol else None),
                "period": (r.get(percol) if percol else None),
                "cost_code": (r.get(codecol) if codecol else None),
                "category": (r.get(catcol) if catcol else None),
                "budget_sar": float(r["_budget"]),
                "actual_sar": float(r["_actual"]),
                "variance_sar": float(r["_variance"]),
                "variance_pct": float(r["_variance_pct"]),
            })
    else:
        r = work[["_budget", "_actual", "_variance"]].sum()
        var_pct = float((r["_variance"] / r["_budget"] * 100.0) if r["_budget"] else 0.0)
        rows = [{
            "project_id": None,
            "period": None,
            "cost_code": None,
            "category": None,
            "budget_sar": float(r["_budget"]),
            "actual_sar": float(r["_actual"]),
            "variance_sar": float(r["_variance"]),
            "variance_pct": var_pct,
        }]

    return {
        "mode": "variance",
        "items": rows,
        "columns_used": {"budget": bcol, "actual": acol, "project": pcol,
                         "period": percol, "cost_code": codecol, "category": catcol},
    }

def summarize_only(df: pd.DataFrame) -> Dict[str, Any]:
    # Provide a very light summary for files without budget/actual
    cols = list(df.columns)
    sample = df.head(5).to_dict(orient="records")
    return {
        "mode": "summary_only",
        "reason": "no_budget_actual_detected",
        "columns_seen": cols,
        "sample_rows": sample,
        "row_count": int(df.shape[0]),
    }

def parse_single_file(content: bytes, filename: str) -> Dict[str, Any]:
    """
    Accepts CSV/Excel/Text. (PDF/Word are handled upstream before calling this.)
    Tries to compute variances if both budget & actual columns exist; otherwise returns a summary.
    """
    name = filename.lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        elif name.endswith(".xlsx") or name.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(content))
        else:
            # generic text/tsv
            df = pd.read_csv(io.BytesIO(content), sep=None, engine="python")
    except Exception:
        # last resort: try to read as free-form text into one column
        text = content.decode("utf-8", errors="ignore")
        lines = [line for line in text.splitlines() if line.strip()]
        df = pd.DataFrame({"text": lines})

    # Strip spaces from headers
    df.columns = [c.strip() for c in df.columns]

    outcome = detect_and_compute_variances(df)
    if outcome.get("mode") == "variance":
        return {"ok": True, "result": outcome}
    else:
        # no budget/actual — just summarize
        return {"ok": True, "result": summarize_only(df)}
