from fastapi import APIRouter, UploadFile, File
import asyncio
import os

from app.parsers.procurement_pdf import parse_procurement_pdf
from app.services.singlefile import process_single_file
from app.parsers.single_file_intake import parse_single_file
from app.services.insights import compute_procurement_insights

router = APIRouter()


@router.post("/drafts/from-file")
async def from_file(file: UploadFile = File(...)):
    """
    Behavior:
      - If the uploaded file contains budget/actuals → return variance.
      - Otherwise, if it contains procurement lines → return procurement summary + insights.
      - If neither detected → return a helpful error message.
    """
    try:
        data = await file.read()
        is_pdf = file.filename.lower().endswith(".pdf")

        if is_pdf:
            # Parse PDFs in a worker thread with a hard timeout to avoid hangs/502s.
            pdf_timeout = int(os.getenv("PDF_PARSE_TIMEOUT", "45"))
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(parse_procurement_pdf, data),
                    timeout=pdf_timeout,
                )
            except asyncio.TimeoutError:
                return {"error": f"PDF parsing timed out after {pdf_timeout} seconds."}

            items = (result or {}).get("items") or []
            if items:
                insights = compute_procurement_insights(items)
                return {
                    "kind": "procurement",
                    "message": "No budget/actuals detected — showing procurement summary.",
                    "procurement_summary": {"items": items},
                    "insights": insights,
                    "meta": (result or {}).get("meta", {}),
                }
            return {
                "error": "We didn’t find budget/actuals or recognizable procurement lines in this PDF."
            }

        # Non-PDF (CSV/Excel/Word/Text)
        parsed = await asyncio.to_thread(parse_single_file, file.filename, data)
        parsed = parsed or {}

        variance_items = parsed.get("variance_items")
        if variance_items:
            return {
                "kind": "variance",
                "variance_items": variance_items,
                "insights": parsed.get("insights", {}),
                "diagnostics": parsed.get("diagnostics"),
            }

        items = (parsed.get("procurement_summary") or {}).get("items") or []
        if items:
            insights = compute_procurement_insights(items)
            return {
                "kind": "procurement",
                "message": "No budget/actuals detected — showing procurement summary.",
                "procurement_summary": {"items": items},
                "insights": insights,
                "diagnostics": parsed.get("diagnostics"),
            }

        # Fallback: try the workbook insights pipeline
        processed = await asyncio.to_thread(process_single_file, file.filename, data)
        processed = processed or {}
        if processed.get("mode") == "insights":
            return {
                "kind": "insights",
                "insights": processed.get("insights", {}),
                "diagnostics": processed.get("diagnostics"),
                "message": processed.get("message"),
            }

        return {
            "error": "This file does not include budget/actuals or procurement lines I can read."
        }

    except Exception as e:
        # Never bubble up to a 502 — surface as a structured response the UI can display.
        return {"error": f"single-file parse failed: {type(e).__name__}: {e}"}

