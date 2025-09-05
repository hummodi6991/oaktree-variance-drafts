from __future__ import annotations

import io
from typing import Dict, Any, List, Optional

import pandas as pd

from app.utils.diagnostics import DiagnosticContext
from app.services.doors_quotes_adapter import (
    is_doors_quotes_workbook,
    adapt as adapt_doors_quotes,
)
from app.services.insights import (
    generate_insights_for_workbook,
    compute_procurement_insights,
    compute_variance_insights,
    summarize_procurement_lines,
)
from app.gpt_client import summarize_financials
from app.parsers.single_file_intake import parse_single_file


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

def _fmt_currency(v: Optional[float]) -> str:
    try:
        if v is None:
            return "—"
        return f"SAR {float(v):,.0f}"
    except Exception:
        return "—"

def _md_table(headers: List[str], rows: List[List[Any]], max_rows: int = 10) -> str:
    if not rows:
        return "_No data found._"
    rows = rows[:max_rows]
    header = " | ".join(headers)
    sep = " | ".join(["---"] * len(headers))
    body = "\n".join(" | ".join(str(c) if c is not None else "—" for c in r) for r in rows)
    return f"{header}\n{sep}\n{body}"

def _build_report_markdown_for_quote_compare(payload: Dict[str, Any], filename: str) -> str:
    """Markdown report for doors-quotes style comparison."""
    vendor_totals = payload.get("vendor_totals", []) or payload.get("totals_per_vendor", [])
    spreads = payload.get("variance_items", [])
    insights = (payload.get("insights") or {})

    # Normalize vendor_totals row shape
    vt_rows = []
    vendor_stats = []  # (vendor, total, subtotal, vat)
    for r in vendor_totals:
        vendor = r.get("vendor") or r.get("vendor_name") or r.get("name") or "Vendor"
        total = r.get("total_amount_sar") or r.get("total_sar") or r.get("total") or r.get("amount_sar")
        sub = r.get("subtotal_amount_sar") or r.get("subtotal_sar") or r.get("subtotal")
        vat = r.get("vat_amount_sar") or r.get("vat_sar") or r.get("vat")
        vt_rows.append([str(vendor), _fmt_currency(total)])
        try:
            vendor_stats.append((str(vendor), float(total) if total is not None else None, sub, vat))
        except Exception:
            vendor_stats.append((str(vendor), None, sub, vat))

    # Normalize spread rows
    sp_rows = []
    for r in spreads:
        item = r.get("item_code") or r.get("description") or r.get("label") or "Item"
        min_u = r.get("min_unit_price_sar") or r.get("min_unit_sar") or r.get("min_price")
        max_u = r.get("max_unit_price_sar") or r.get("max_unit_sar") or r.get("max_price")
        spread = r.get("unit_price_spread_sar") or r.get("spread_sar") or r.get("delta_sar")
        spread_p = r.get("unit_price_spread_pct") or r.get("spread_pct") or r.get("delta_pct")
        sp_rows.append([str(item)[:40], _fmt_currency(min_u), _fmt_currency(max_u), _fmt_currency(spread), f"{spread_p:.1f}%" if isinstance(spread_p,(int,float)) else "—"])

    # Summary bullets
    n_vendors = len(vt_rows)
    total_spend = None
    try:
        totals = []
        for r in vendor_totals:
            t = r.get("total_amount_sar") or r.get("total_sar") or r.get("total") or r.get("amount_sar")
            if t is not None:
                totals.append(float(t))
        total_spend = sum(totals) if totals else None
    except Exception:
        total_spend = None

    highlights = insights.get("highlights") or []
    bullets = []
    if total_spend is not None:
        bullets.append(f"**Total quoted spend:** {_fmt_currency(total_spend)}")
    bullets.append(f"**Vendors compared:** {n_vendors or '—'}")

    # Cost comparison
    stats = [vs for vs in vendor_stats if vs[1] is not None]
    if stats:
        lowest = min(stats, key=lambda t: t[1])
        highest = max(stats, key=lambda t: t[1])
        diff = highest[1] - lowest[1]
        bullets.append(
            f"**Lowest total quote:** {lowest[0]} ({_fmt_currency(lowest[1])})"
        )
        bullets.append(
            f"**Highest total quote:** {highest[0]} ({_fmt_currency(highest[1])})"
        )
        bullets.append(
            f"**Difference:** {_fmt_currency(diff)} between highest and lowest bids"
        )
        bullets.append(
            f"**Recommendation:** {lowest[0]} offers the lowest total; verify quality and scope before awarding"
        )

    if spreads:
        bullets.append("**Price dispersion detected** across vendors (see table below).")
    bullets.extend(f"- {h}" for h in highlights[:5])

    md = []
    md.append(f"### Single-File Summary — {filename}")
    md.append("")
    if bullets:
        md.append("#### Overview")
        md.append("\n".join(f"- {b}" if not b.startswith("- ") else b for b in bullets))
        md.append("")

    # Vendor summaries with optional subtotal/VAT
    vendor_lines = []
    for v, tot, sub, vat in vendor_stats:
        parts = []
        if sub is not None:
            parts.append(f"Subtotal {_fmt_currency(sub)}")
        if vat is not None:
            parts.append(f"VAT {_fmt_currency(vat)}")
        parts.append(f"Total {_fmt_currency(tot)}")
        vendor_lines.append(f"{v}: "+", ".join(parts))

    if vendor_lines:
        md.append("#### Vendor summaries")
        md.append("\n".join(f"- {line}" for line in vendor_lines))
        md.append("")

    if vt_rows:
        md.append("#### Totals by vendor")
        md.append(_md_table(["Vendor", "Total"], vt_rows))
        md.append("")
    if sp_rows:
        md.append("#### Top unit price spreads")
        md.append(_md_table(["Item", "Min Unit", "Max Unit", "Spread", "Spread %"], sp_rows))
        md.append("")
    return "\n".join(md).strip()

def _build_report_markdown_for_generic_insights(insights: Dict[str, Any], filename: str) -> str:
    """Markdown report for generic workbook insights (CSV/XLSX)."""
    highlights = insights.get("highlights") or []
    vendor_tbl = insights.get("tables", {}).get("workbook::vendor_totals") if isinstance(insights.get("tables"), dict) else None
    spread_tbl = insights.get("tables", {}).get("workbook::vendor_spreads") if isinstance(insights.get("tables"), dict) else None

    vt_rows: List[List[Any]] = []
    if vendor_tbl and isinstance(vendor_tbl, list):
        for r in vendor_tbl:
            vt_rows.append([str(r.get("vendor") or r.get("vendor_name") or "Vendor"), _fmt_currency(r.get("total_sar"))])

    sp_rows: List[List[Any]] = []
    if spread_tbl and isinstance(spread_tbl, list):
        for r in spread_tbl:
            sp_rows.append([str(r.get("item") or r.get("label") or "Item")[:40],
                            _fmt_currency(r.get("min_unit_sar")),
                            _fmt_currency(r.get("max_unit_sar")),
                            _fmt_currency(r.get("spread_sar")),
                            f"{r.get('spread_pct', 0):.1f}%" if isinstance(r.get("spread_pct"), (int,float)) else "—"])

    md = [f"### Single-File Summary — {filename}", ""]
    if highlights:
        md.append("#### Highlights")
        md.append("\n".join(f"- {h}" for h in highlights[:8]))
        md.append("")
    if vt_rows:
        md.append("#### Totals by vendor")
        md.append(_md_table(["Vendor","Total"], vt_rows))
        md.append("")
    if sp_rows:
        md.append("#### Vendor price spreads")
        md.append(_md_table(["Item","Min Unit","Max Unit","Spread","Spread %"], sp_rows))
        md.append("")
    return "\n".join(md).strip()


def _build_report_markdown_for_variance(insights: Dict[str, Any], filename: str) -> str:
    """Markdown report for budget vs actual variance extracted from PDFs."""
    totals = insights.get("totals", {}) if isinstance(insights, dict) else {}
    over = insights.get("top_overruns", []) if isinstance(insights, dict) else []
    under = insights.get("top_underruns", []) if isinstance(insights, dict) else []

    bullets = []
    if totals.get("budget_sar") is not None:
        bullets.append(f"**Total budget:** {_fmt_currency(totals.get('budget_sar'))}")
    if totals.get("actual_sar") is not None:
        bullets.append(f"**Total actual:** {_fmt_currency(totals.get('actual_sar'))}")
    if totals.get("variance_sar") is not None:
        bullets.append(f"**Variance:** {_fmt_currency(totals.get('variance_sar'))}")

    over_rows = [
        [str(r.get("label") or "Item"), _fmt_currency(r.get("variance_sar"))]
        for r in over
    ]
    under_rows = [
        [str(r.get("label") or "Item"), _fmt_currency(r.get("variance_sar"))]
        for r in under
    ]

    md: List[str] = [f"### Single-File Summary — {filename}", ""]
    if bullets:
        md.append("#### Totals")
        md.append("\n".join(f"- {b}" for b in bullets))
        md.append("")
    if over_rows:
        md.append("#### Top overruns")
        md.append(_md_table(["Item", "Variance"], over_rows))
        md.append("")
    if under_rows:
        md.append("#### Top underruns")
        md.append(_md_table(["Item", "Variance"], under_rows))
        md.append("")
    return "\n".join(md).strip()


def process_single_file(
    filename: str,
    data: bytes,
    materiality_pct: float = DEFAULT_MATERIALITY_PCT,
    materiality_amt_sar: float = DEFAULT_MATERIALITY_AMT_SAR,
) -> Dict[str, Any]:
    """Simplified single-file parser returning quote comparison, workbook or PDF insights."""
    name = (filename or "").lower()

    if name.endswith(".pdf"):
        parsed = parse_single_file(filename, data) or {}

        variance = parsed.get("variance_items") or []
        if variance:
            insights = compute_variance_insights(variance)
            return {
                "mode": "variance",
                "variance_items": variance,
                "insights": insights,
                "analysis": insights,
                "report_markdown": _build_report_markdown_for_variance(insights, filename),
                "diagnostics": parsed.get("diagnostics", {}),
            }

        items = (parsed.get("procurement_summary") or {}).get("items") or []
        analysis = (
            parsed.get("analysis")
            or parsed.get("economic_analysis")
            or compute_procurement_insights(items)
        )
        insights = parsed.get("insights") or analysis
        if isinstance(insights, dict):
            insights.pop("cards", None)
            insights.pop("tables", None)
        summary = summarize_procurement_lines(items)
        highs = summary.get("highlights") or []
        if highs and isinstance(insights, dict):
            insights = {**insights, "highlights": highs}
        summary_text = summarize_financials(summary, insights if isinstance(insights, dict) else {})
        return {
            "summary": summary,
            "analysis": analysis,
            "insights": insights,
            "summary_text": summary_text,
            "diagnostics": parsed.get("diagnostics", {}),
        }

    with DiagnosticContext(file_name=filename, file_size=len(data)) as diag:
        diag.step("singlefile_start", filename=filename)
        wb = _load_workbook(filename, data)

        if isinstance(wb, pd.ExcelFile):
            diag.step("read_excel_success", sheets=list(wb.sheet_names))
            try:
                if is_doors_quotes_workbook(wb, file_name=filename):
                    diag.step("doors_quotes_detected")
                    payload = adapt_doors_quotes(wb, materiality_pct, materiality_amt_sar)
                    # Attach reader-friendly analysis & report
                    payload["report_markdown"] = _build_report_markdown_for_quote_compare(payload, filename)
                    payload.setdefault("insights", {})
                    payload["insights"].setdefault("highlights", [])
                    payload["diagnostics"] = diag.to_dict()
                    diag.step("report_built", length=len(payload["report_markdown"]))
                    return payload
            except Exception as e:  # pragma: no cover - defensive
                diag.warn("doors_quotes_detection_failed", error=str(e))

            try:
                sheets = {sn: wb.parse(sn) for sn in wb.sheet_names}
                insights = generate_insights_for_workbook(sheets)
                if isinstance(insights, dict):
                    insights.pop("cards", None)
                    insights.pop("tables", None)
                summary_text = summarize_financials({}, insights if isinstance(insights, dict) else {})
                result = {
                    "summary": {},
                    "analysis": insights,
                    "insights": insights,
                    "summary_text": summary_text,
                    "diagnostics": diag.to_dict(),
                }
                return result
            except Exception as e:  # pragma: no cover - defensive
                diag.error("insights_failed", e)
                insights = {"highlights": ["Unable to summarize this workbook."]}
                return {
                    "summary": {},
                    "analysis": insights,
                    "insights": insights,
                    "summary_text": summarize_financials({}, insights),
                    "diagnostics": diag.to_dict(),
                }

        if isinstance(wb, dict) and "__csv__" in wb:
            df = wb["__csv__"]
            diag.step("read_csv_success", rows=int(df.shape[0]), cols=int(df.shape[1]))
            insights = generate_insights_for_workbook({"Sheet1": df})
            if isinstance(insights, dict):
                insights.pop("cards", None)
                insights.pop("tables", None)
            result = {
                "summary": {},
                "analysis": insights,
                "insights": insights,
                "summary_text": summarize_financials({}, insights if isinstance(insights, dict) else {}),
                "diagnostics": diag.to_dict(),
            }
            return result

        diag.warn("unsupported_or_empty", filename=filename)
        insights = {"highlights": ["Unsupported or empty file."]}
        return {
            "summary": {},
            "analysis": insights,
            "insights": insights,
            "summary_text": summarize_financials({}, insights),
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

