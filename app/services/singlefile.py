from __future__ import annotations

import io
from typing import Dict, Any

import pandas as pd

from app.utils.diagnostics import DiagnosticContext
from app.services.doors_quotes_adapter import (
    is_doors_quotes_workbook,
    adapt as adapt_doors_quotes,
)
from app.services.insights import generate_insights_for_workbook


DEFAULT_MATERIALITY_PCT = 5.0
DEFAULT_MATERIALITY_AMT_SAR = 100_000.0


def _load_workbook(filename: str, data: bytes):
    """Return a :class:`pandas.ExcelFile` for Excel uploads or a dict for CSV."""
    name = (filename or "").lower()
    if name.endswith((".xlsx", ".xls")):
        return pd.ExcelFile(io.BytesIO(data))
    if name.endswith(".csv"):
        return {"__csv__": pd.read_csv(io.BytesIO(data))}
    # Fallback: try Excel; caller handles failure
    try:
        return pd.ExcelFile(io.BytesIO(data))
    except Exception:
        return {}


def process_single_file(
    filename: str,
    data: bytes,
    materiality_pct: float = DEFAULT_MATERIALITY_PCT,
    materiality_amt_sar: float = DEFAULT_MATERIALITY_AMT_SAR,
) -> Dict[str, Any]:
    """Simplified single-file parser returning quote comparison or workbook insights."""
    with DiagnosticContext(file_name=filename, file_size=len(data)) as diag:
        diag.step("singlefile_start", filename=filename)
        wb = _load_workbook(filename, data)

        if isinstance(wb, pd.ExcelFile):
            diag.step("read_excel_success", sheets=list(wb.sheet_names))
            try:
                if is_doors_quotes_workbook(wb, file_name=filename):
                    diag.step("doors_quotes_detected")
                    payload = adapt_doors_quotes(wb, materiality_pct, materiality_amt_sar)
                    payload["diagnostics"] = diag.to_dict()
                    return payload
            except Exception as e:  # pragma: no cover - defensive
                diag.warn("doors_quotes_detection_failed", error=str(e))

            try:
                sheets = {sn: wb.parse(sn) for sn in wb.sheet_names}
                insights = generate_insights_for_workbook(sheets)
                return {
                    "mode": "insights",
                    "insights": insights,
                    "diagnostics": diag.to_dict(),
                }
            except Exception as e:  # pragma: no cover - defensive
                diag.error("insights_failed", e)
                return {
                    "mode": "insights",
                    "insights": {"highlights": ["Unable to summarize this workbook."]},
                    "diagnostics": diag.to_dict(),
                }

        if isinstance(wb, dict) and "__csv__" in wb:
            df = wb["__csv__"]
            diag.step("read_csv_success", rows=int(df.shape[0]), cols=int(df.shape[1]))
            insights = generate_insights_for_workbook({"Sheet1": df})
            return {
                "mode": "insights",
                "insights": insights,
                "diagnostics": diag.to_dict(),
            }

        diag.warn("unsupported_or_empty", filename=filename)
        return {
            "mode": "insights",
            "insights": {"highlights": ["Unsupported or empty file."]},
            "diagnostics": diag.to_dict(),
        }


def draft_bilingual_procurement_card(it: Dict[str, Any], file_label: str) -> Dict[str, str]:
    """Return a tiny bilingual summary for a procurement line item."""
    parts_en = []
    code = it.get("item_code")
    desc = it.get("description")
    qty = it.get("qty")
    upr = it.get("unit_price_sar")
    amt = it.get("amount_sar")
    ven = it.get("vendor")
    dt = it.get("doc_date")
    if code:
        parts_en.append(f"Item: {code}")
    if desc:
        parts_en.append(f"Description: {desc}")
    if qty is not None:
        parts_en.append(f"Quantity: {qty}")
    if upr is not None:
        parts_en.append(f"Unit price (SAR): {upr}")
    if amt is not None:
        parts_en.append(f"Line total (SAR): {amt}")
    if ven:
        parts_en.append(f"Vendor: {ven}")
    if dt:
        parts_en.append(f"Document date: {dt}")
    parts_en.append(f"Evidence: {file_label}")

    parts_ar = []
    if code:
        parts_ar.append(f"البند: {code}")
    if desc:
        parts_ar.append(f"الوصف: {desc}")
    if qty is not None:
        parts_ar.append(f"الكمية: {qty}")
    if upr is not None:
        parts_ar.append(f"سعر الوحدة (ريال): {upr}")
    if amt is not None:
        parts_ar.append(f"الإجمالي (ريال): {amt}")
    if ven:
        parts_ar.append(f"المورد: {ven}")
    if dt:
        parts_ar.append(f"تاريخ المستند: {dt}")
    parts_ar.append(f"الدليل: {file_label}")

    return {"en": " | ".join(parts_en), "ar": " | ".join(parts_ar)}

