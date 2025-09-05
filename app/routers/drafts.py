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
        if ps:
            # Build compact payload for LLM
            totals = {
                "grand_total": parsed.get("grand_total_sar") or ps_full.get("grand_total_sar"),
                "vat_amount": parsed.get("vat_amount_sar") or ps_full.get("vat_amount_sar"),
                "subtotal": parsed.get("subtotal_sar") or ps_full.get("subtotal_sar"),
                "line_count": len(ps),
                "vendor_count": len({(r.get("vendor_name") or "").strip() for r in ps if r.get("vendor_name")}),
            }
            vendors = []
            try:
                vendors = compute_procurement_insights(ps, basket=DEFAULT_BASKET).get("totals_per_vendor", [])
            except Exception:
                vendors = []

            llm_out = llm_financial_summary({
                "lines": ps,
                "vendors": vendors,
                "totals": totals,
                "raw_text": parsed.get("raw_text", ""),
            })

            # Also keep machine summary objects if callers need them, but UI prints text only
            return {
                "kind": "insights",
                "summary": {"totals": totals, "vendors": vendors},
                "analysis": {"text": llm_out.get("analysis_text", "")},
                "insights": {"text": llm_out.get("insights_text", "")},
                "summary_text": llm_out.get("summary_text", ""),
            }

        return {
            "error": "We couldn’t find budget/actuals or recognizable procurement lines in this file.",
        }
    except Exception as e:
        return {"error": str(e)}

