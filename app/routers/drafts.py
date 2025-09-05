from fastapi import APIRouter, UploadFile, File
import asyncio
from app.parsers.single_file_intake import parse_single_file
from app.services.insights import (
    compute_procurement_insights,
    compute_variance_insights,
    summarize_procurement_lines,
    DEFAULT_BASKET,
)
from app.gpt_client import summarize_financials
from app.services.llm import llm_financial_summary
import re


def _rough_totals_from_text(text: str):
    """Extract rough subtotal/VAT/total from noisy PDF text (best-effort).
    Returns dict with any of: subtotal, vat_amount, grand_total, line_count, vendor_count."""
    if not text:
        return {}

    def _num(s):
        s = s.replace(",", "")
        try:
            return float(re.findall(r"\d+(?:\.\d+)?", s)[0])
        except Exception:
            return None

    totals = {}
    m_total = re.search(r"(?i)(grand\s*total|total\s+amount|total)\D{0,24}(\d[\d,]*(?:\.\d+)?)", text)
    if m_total:
        v = _num(m_total.group(2))
        if v is not None:
            totals["grand_total"] = v
    m_sub = re.search(r"(?i)(sub\s*total|subtotal)\D{0,24}(\d[\d,]*(?:\.\d+)?)", text)
    if m_sub:
        v = _num(m_sub.group(2))
        if v is not None:
            totals["subtotal"] = v
    m_vat = re.search(r"(?i)(VAT|value\s*added\s*tax)\D{0,24}(\d[\d,]*(?:\.\d+)?)", text)
    if m_vat:
        v = _num(m_vat.group(2))
        if v is not None:
            totals["vat_amount"] = v
    return totals

router = APIRouter()


@router.post("/drafts/from-file")
async def from_file(file: UploadFile = File(...)):
    """
    Unified single-file track for ANY file (including PDFs):
      • If Budget/Actual pairs are found → return variance (+ analysis/insights).
      • Else if procurement lines exist → return summary (+ economic analysis/insights).
      • Else → return a helpful error and diagnostics.
    """
    try:
        data = await file.read()
        parsed = await asyncio.to_thread(parse_single_file, file.filename, data)
        parsed = parsed or {}

        # Path A: Variance detected
        variance = parsed.get("variance_items") or []
        if variance:
            return {
                "kind": "variance",
                "variance_items": variance,
                "insights": compute_variance_insights(variance),
            }

        # Path B: No variance detected → return summary + analysis + insights ONLY (no cards/diagnostics)
        ps_full = parsed.get("procurement_summary") or {}
        ps = ps_full.get("items") or []
        raw_text = parsed.get("raw_text", "") or ""
        # Build compact payload for LLM (even if ps is empty)
        totals = {
            "grand_total": parsed.get("grand_total_sar") or ps_full.get("grand_total_sar"),
            "vat_amount": parsed.get("vat_amount_sar") or ps_full.get("vat_amount_sar"),
            "subtotal": parsed.get("subtotal_sar") or ps_full.get("subtotal_sar"),
            "line_count": len(ps) if ps else None,
            "vendor_count": (
                len({(r.get("vendor_name") or '').strip() for r in ps if r.get("vendor_name")})
                if ps
                else None
            ),
        }
        # If parser found nothing, try rough totals from raw text
        if not any([totals.get("grand_total"), totals.get("subtotal"), totals.get("vat_amount")]):
            totals.update(_rough_totals_from_text(raw_text))

        vendors = []
        try:
            if ps:
                vendors = compute_procurement_insights(ps, basket=DEFAULT_BASKET).get(
                    "totals_per_vendor", []
                )
        except Exception:
            vendors = []

        llm_out = llm_financial_summary(
            {
                "lines": ps,
                "vendors": vendors,
                "totals": totals,
                "raw_text": raw_text,
            }
        )

        # Fallback text if model not reachable
        if not (llm_out.get("summary_text") or "").strip():
            bullets = []
            if totals.get("grand_total") is not None:
                bullets.append(f"Estimated total: SAR {totals['grand_total']:.2f}.")
            if totals.get("subtotal") is not None and totals.get("vat_amount") is not None:
                bullets.append(
                    f"Subtotal \u2248 SAR {totals['subtotal']:.2f}; VAT \u2248 SAR {totals['vat_amount']:.2f}."
                )
            if totals.get("line_count"):
                bullets.append(f"Detected about {totals['line_count']} line items.")
            llm_out["summary_text"] = ("Summary\n" + " ".join(bullets)).strip()
            llm_out["analysis_text"] = (
                "Financial analysis\nBased on the detected figures, costs are concentrated in a small set of items; verify unit prices, quantities, and VAT applicability."
            )
            llm_out["insights_text"] = (
                "Financial insights\nValidate vendor terms, ensure PO references are consistent, and confirm whether VAT is included in unit rates."
            )

        return {
            "kind": "insights",
            "summary": {"totals": totals, "vendors": vendors},
            "analysis": {"text": llm_out.get("analysis_text", "")},
            "insights": {"text": llm_out.get("insights_text", "")},
            "summary_text": llm_out.get("summary_text", ""),
        }

    except Exception as e:
        return {"error": str(e)}

