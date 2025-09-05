from typing import Dict, Any, List
import io
import re
import pdfplumber
import docx
import pandas as pd

from app.services.insights import (
    compute_procurement_insights,
    summarize_procurement_lines,
    DEFAULT_BASKET,
)
from app.gpt_client import summarize_financials

RE_MONEY = re.compile(r"(?<![\d.])(?:SAR|SR|ر\.س)?\s*([0-9]{1,3}(?:[,0-9]{0,3})*(?:\.[0-9]{1,2})?)", re.I)
RE_DATE  = re.compile(r"(20\d{2}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}[/-]\d{1,2}[/-]20\d{2})")


def _norm_money(s: str) -> float:
    m = RE_MONEY.search(s or "")
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def _read_anytable_to_df(data: bytes, name: str) -> List[pd.DataFrame]:
    ext = (name or "").lower()
    dfs = []
    try:
        if ext.endswith(".csv"):
            dfs = [pd.read_csv(io.BytesIO(data))]
        elif ext.endswith((".xlsx", ".xls")):
            xl = pd.ExcelFile(io.BytesIO(data))
            dfs = [xl.parse(s) for s in xl.sheet_names]
        elif ext.endswith(".pdf"):
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                for page in pdf.pages:
                    tbls = page.extract_tables()
                    for t in tbls or []:
                        df = pd.DataFrame(t)
                        if df.shape[1] >= 3:
                            dfs.append(df)
        elif ext.endswith((".docx", ".doc")):
            d = docx.Document(io.BytesIO(data))
            for tbl in d.tables:
                rows = [[c.text.strip() for c in row.cells] for row in tbl.rows]
                df = pd.DataFrame(rows)
                if df.shape[1] >= 3:
                    dfs.append(df)
    except Exception:
        pass
    return dfs


def _detect_budget_actual(text: str) -> bool:
    # very tolerant: look for "budget" and "actual" terms anywhere
    return ("budget" in text.lower()) and ("actual" in text.lower())


def _pdf_text(data: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n".join([p.extract_text() or "" for p in pdf.pages])
    except Exception:
        return ""


def _extract_proc_lines(dfs: List[pd.DataFrame], text: str) -> List[Dict[str, Any]]:
    """
    Extract procurement lines safely (no invention).
    We try to find columns resembling qty / unit / unit price / total / item code / description.
    """
    lines: List[Dict[str, Any]] = []
    # Try structured tables first
    for df in dfs:
        df2 = df.copy()
        df2.columns = [str(c).strip().lower() for c in df2.iloc[0].tolist()]
        df2 = df2.iloc[1:].reset_index(drop=True)
        has_qty = any("qty" in c for c in df2.columns)
        # accept many flavors
        price_cols = [c for c in df2.columns if ("unit price" in c) or ("u. rate" in c) or ("price" == c)]
        total_cols = [c for c in df2.columns if ("total" in c) and ("vat" not in c)]
        desc_cols = [c for c in df2.columns if "description" in c or "item description" in c]
        code_cols = [c for c in df2.columns if "item code" in c or c in ("code", "item")]
        if not (has_qty and (price_cols or total_cols) and (desc_cols or code_cols)):
            continue
        for _, r in df2.iterrows():
            qty = r.get(next((c for c in df2.columns if "qty" in c), ""), None)
            unit = r.get(next((c for c in df2.columns if c in ("unit", "units")), ""), None)
            unit_price = r.get(price_cols[0], None) if price_cols else None
            total = r.get(total_cols[0], None) if total_cols else None
            desc = r.get(desc_cols[0], None) if desc_cols else None
            code = r.get(code_cols[0], None) if code_cols else None
            if pd.isna(qty) and pd.isna(total) and pd.isna(unit_price) and not desc:
                continue
            lines.append(
                {
                    "item_code": None if pd.isna(code) else str(code).strip(),
                    "description": None if pd.isna(desc) else str(desc).strip(),
                    "quantity": None if pd.isna(qty) else str(qty).strip(),
                    "unit": None if pd.isna(unit) else str(unit).strip(),
                    "unit_price_sar": _norm_money(str(unit_price)) if unit_price is not None else None,
                    "amount_sar": _norm_money(str(total)) if total is not None else None,
                }
            )
    # If nothing from tables, fall back to simple patterns in text
    if not lines and text:
        # Look for repeated blocks for D01..D04 style
        for block in re.split(r"\n\s*\n", text):
            qty = re.search(r"\b(\d{1,3})\s*(?:pcs|sets?)\b", block, re.I)
            unit = re.search(r"\b(pcs|sets?)\b", block, re.I)
            unit_price = RE_MONEY.search(block)
            total = None
            if "total" in block.lower():
                total = RE_MONEY.findall(block)[-1] if RE_MONEY.findall(block) else None
            code = re.search(r"\bD0\d\b", block)
            desc = block.strip().replace("\n", " ")
            if qty or unit_price or code:
                lines.append(
                    {
                        "item_code": code.group(0) if code else None,
                        "description": desc[:400],
                        "quantity": qty.group(1) if qty else None,
                        "unit": unit.group(1).upper() if unit else None,
                        "unit_price_sar": float(unit_price.group(1)) if unit_price else None,
                        "amount_sar": float(total) if isinstance(total, str) and total.replace(",", "").isdigit() else None,
                    }
                )
    return lines


async def analyze_single_file(
    data: bytes,
    name: str,
    bilingual: bool = True,
    no_speculation: bool = True,
) -> Dict[str, Any]:
    text = ""
    if name.lower().endswith(".pdf"):
        text = _pdf_text(data)
    dfs = _read_anytable_to_df(data, name)

    # Decide mode
    contains_ba = _detect_budget_actual(text)

    if contains_ba:
        # Minimal, safe variance: read a two-column budget/actual if present; otherwise return empty.
        # (We keep this conservative to avoid inventing.)
        frames = dfs or []
        insights: List[Dict[str, Any]] = []
        for df in frames:
            low = [str(c).strip().lower() for c in df.iloc[0].tolist()] if df.shape[0] else []
            if {"budget", "actual"}.issubset(set(low)):
                df2 = df.copy()
                df2.columns = low
                df2 = df2.iloc[1:].reset_index(drop=True)
                for _, r in df2.iterrows():
                    try:
                        b = float(str(r.get("budget", "0")).replace(",", ""))
                        a = float(str(r.get("actual", "0")).replace(",", ""))
                        if b == 0 and a == 0:
                            continue
                        insights.append(
                            {
                                "label": str(r.get("label") or r.get("item") or "Line"),
                                "budget_sar": b,
                                "actual_sar": a,
                                "variance_sar": a - b,
                            }
                        )
                    except Exception:
                        continue
        return {"report_type": "variance_insights", "items": insights, "source": name}

    # Otherwise => Procurement summary only
    lines = _extract_proc_lines(dfs, text)
    # Try to find vendor & date
    vendor = None
    for v in re.findall(
        r"(Admark Creative|AL\s*AZAL|Modern Furnishing House|Woodwork Arts|Burj\s+Al\s+Ekha)",
        text,
        re.I,
    ):
        vendor = v if v else vendor
    date = None
    mdate = RE_DATE.search(text)
    if mdate:
        date = mdate.group(1)
    cards = []
    for L in lines:
        cards.append(
            {
                "item_code": L.get("item_code"),
                "description": L.get("description"),
                "quantity": L.get("quantity"),
                "unit": L.get("unit"),
                "unit_price_sar": L.get("unit_price_sar"),
                "amount_sar": L.get("amount_sar"),
                "vendor": vendor,
                "doc_date": date,
                "source": "Uploaded procurement file",
            }
        )
    analysis = compute_procurement_insights(cards, basket=DEFAULT_BASKET)
    summary = summarize_procurement_lines(cards)
    return summarize_financials(summary, analysis)

