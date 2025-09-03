from __future__ import annotations
import io, re, json, csv
from typing import Dict, Any, List, Tuple
import chardet
import pandas as pd
from pypdf import PdfReader
import pdfplumber
from docx import Document

REQUIRED_VARIANCE_HINTS = ("budget", "actual")


def _read_csv_bytes(b: bytes) -> pd.DataFrame:
    enc = chardet.detect(b).get("encoding") or "utf-8"
    return pd.read_csv(io.BytesIO(b), encoding=enc)


def _read_excel_bytes(b: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(b), engine="openpyxl")


def _read_docx_bytes(b: bytes) -> str:
    fp = io.BytesIO(b)
    doc = Document(fp)
    return "\n".join(p.text for p in doc.paragraphs)


def _read_pdf_text(b: bytes) -> str:
    # try structured text first
    try:
        with pdfplumber.open(io.BytesIO(b)) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception:
        # simple fallback
        rd = PdfReader(io.BytesIO(b))
        return "\n".join(p.extract_text() or "" for p in rd.pages)


def _maybe_variance_from_tabular(df: pd.DataFrame) -> Dict[str, Any] | None:
    # very tolerant: lower, strip, underscore columns
    def norm(c: str) -> str:
        return re.sub(r"\W+", "_", str(c).strip().lower())
    df = df.rename(columns={c: norm(c) for c in df.columns})
    cols = set(df.columns)
    has_budget = any("budget" in c for c in cols)
    has_actual = any("actual" in c for c in cols)
    if not (has_budget and has_actual):
        return None

    # pick first matching columns
    budget_col = next(c for c in df.columns if "budget" in c)
    actual_col = next(c for c in df.columns if "actual" in c)
    # group by best-available key: cost_code, category, or project_id
    for key in ("cost_code", "category", "project_id"):
        if key in df.columns:
            group_key = key
            break
    else:
        group_key = None

    if group_key:
        g = df.groupby(group_key, dropna=False)[[budget_col, actual_col]].sum().reset_index()
    else:
        g = df[[budget_col, actual_col]].sum().to_frame().T
        g.insert(0, "label", "Total")
        group_key = "label"

    g["variance_sar"] = g[actual_col] - g[budget_col]
    g["variance_pct"] = (g["variance_sar"] / g[budget_col].replace(0, pd.NA)) * 100
    items = []
    for _, r in g.fillna(0).iterrows():
        items.append({
            "label": r[group_key],
            "budget_sar": float(r[budget_col]),
            "actual_sar": float(r[actual_col]),
            "variance_sar": float(r["variance_sar"]),
            "variance_pct": float(r["variance_pct"]) if r["variance_pct"] == r["variance_pct"] else None,
        })
    return {"mode": "variance", "items": items}


_RE_ITEM = re.compile(r"\b(D0?\d)\b", re.I)
_RE_QTY = re.compile(r"\b(\d{1,4})\s*(pcs|sets?)\b", re.I)
_RE_UNIT = re.compile(r"(?:unit\s*price|u\.?\s*rate)\D+([\d,]+\.\d{2}|\d{1,7})", re.I)
_RE_TOTAL = re.compile(r"(?:total(?:\s*in\s*sar)?|amount)\D+([\d,]+\.\d{2}|\d{1,9})", re.I)
_RE_VAT = re.compile(r"\bVAT\b.*?(\d{1,2})\s*%|\bTotal with Vat\b.*?([\d,]+\.\d{2})", re.I)
_RE_VENDOR = re.compile(r"(Admark Creative Co\.|Al Azal(?: Est\.?)?|Woodwork Arts|Modern Furnishing House|Burj[^\n]*Ekha|ALAM)", re.I)
_RE_DATE = re.compile(r"\b(?:Date|DATE)\s*[:\-]?\s*(\d{1,2}\/\d{1,2}\/\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\-\d{1,2}\-\d{4})")


def _procurement_from_text(txt: str) -> Dict[str, Any] | None:
    # Extract vendor quotes and line items D01..Dxx with qty/unit/total
    vendor = None
    date = None
    items: List[Dict[str, Any]] = []
    for line in txt.splitlines():
        if not vendor:
            m = _RE_VENDOR.search(line)
            if m:
                vendor = m.group(1).strip()
        if not date:
            m = _RE_DATE.search(line)
            if m:
                date = m.group(1)

        code = None
        m = _RE_ITEM.search(line)
        if m:
            code = m.group(1).upper()

        qty = None
        m = _RE_QTY.search(line)
        if m:
            qty = int(m.group(1))

        unit = None
        m = _RE_UNIT.search(line)
        if m:
            unit = float(str(m.group(1)).replace(",", ""))

        total = None
        m = _RE_TOTAL.search(line)
        if m:
            total = float(str(m.group(1)).replace(",", ""))

        if code or qty or unit or total:
            items.append(
                {
                    "item_code": code,
                    "qty": qty,
                    "unit_price_sar": unit,
                    "amount_sar": total,
                }
            )

    items = [i for i in items if any(v is not None for v in i.values())]
    if not items and not vendor:
        return None
    return {
        "mode": "procurement",
        "doc_date": date,
        "vendor_name": vendor,
        "items": items,
    }


def parse_single_file(filename: str, data: bytes) -> Dict[str, Any]:
    name = (filename or "").lower()
    # CSV/Excel first (variance track if possible)
    try:
        if name.endswith(".csv"):
            df = _read_csv_bytes(data)
            v = _maybe_variance_from_tabular(df)
            if v:
                return v
        elif name.endswith((".xlsx", ".xls")):
            df = _read_excel_bytes(data)
            v = _maybe_variance_from_tabular(df)
            if v:
                return v
    except Exception:
        pass

    # DOCX/PDF/TXT => procurement or general summary
    if name.endswith(".docx"):
        txt = _read_docx_bytes(data)
    elif name.endswith(".pdf"):
        txt = _read_pdf_text(data)
    else:
        # text-like
        try:
            txt = data.decode("utf-8", errors="ignore")
        except Exception:
            txt = _read_pdf_text(data)

    p = _procurement_from_text(txt)
    if p:
        return p

    # Fallback summary (no speculation): first 2k chars
    snippet = "\n".join(x for x in txt.splitlines() if x).strip()[:2000]
    return {"mode": "summary", "text": snippet}

