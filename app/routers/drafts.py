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
                "diagnostics": parsed.get("diagnostics", {}),
            }

        # Path B: No variance detected → show summary + analysis + insights
        ps_full = parsed.get("procurement_summary") or {}
        ps = ps_full.get("items") or []
        if ps:
            analysis = (
                parsed.get("analysis")
                or parsed.get("economic_analysis")
                or compute_procurement_insights(ps, basket=DEFAULT_BASKET)
            )
            insights = parsed.get("insights") or analysis
            summary = summarize_procurement_lines(ps)
            highs = summary.get("highlights") or []
            if highs and isinstance(insights, dict):
                insights = {**insights, "highlights": highs}
            return summarize_financials(
                summary, insights if isinstance(insights, dict) else {}
            )

        return {
            "error": "We couldn’t find budget/actuals or recognizable procurement lines in this file.",
            "diagnostics": parsed.get("diagnostics", {}),
        }
    except Exception as e:
        return {"error": str(e)}

