from fastapi import APIRouter, UploadFile, File
import asyncio

from app.parsers.procurement_pdf import parse_procurement_pdf
from app.services.singlefile import process_single_file
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
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(parse_procurement_pdf, data),
                    timeout=25,
                )
            except asyncio.TimeoutError:
                return {"error": "PDF parsing timed out after 25 seconds."}

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
        processed = await asyncio.to_thread(process_single_file, file.filename, data)
        processed = processed or {}

        if processed.get("mode") == "variance":
            variance_items = processed.get("variances") or []
            return {
                "kind": "variance",
                "variance_items": variance_items,
                "insights": processed.get("insights", {}),
            }

        # NEW: quote-compare workbooks (e.g., doors_quotes_complete.xlsx)
        # Map the service payload through as a first-class kind so the UI can render it.
        if processed.get("mode") == "quote_compare":
            return {
                "kind": "quote_compare",
                # expose under the stable name the UI expects
                "variance_items": processed.get("variance_items") or [],
                "vendor_totals": processed.get("vendor_totals") or [],
                "insights": processed.get("insights", {}),
                "message": processed.get("message"),
                # pass diagnostics through when present
                "diagnostics": processed.get("diagnostics"),
            }

        # NEW: Workbook-level insights (no budget/actuals and no recognizable line items)
        if processed.get("mode") == "insights":
            return {
                "kind": "insights",
                "insights": processed.get("insights", {}),
                "diagnostics": processed.get("diagnostics"),
                "message": processed.get("message"),
            }

        items = processed.get("items") or []
        if items:
            insights = compute_procurement_insights(items)
            return {
                "kind": "procurement",
                "message": "No budget/actuals detected — showing procurement summary.",
                "procurement_summary": {"items": items},
                "insights": insights,
            }

        return {
            "error": "This file does not include budget/actuals or procurement lines I can read."
        }

    except Exception as e:
        # Never bubble up to a 502 — surface as a structured response the UI can display.
        return {"error": f"single-file parse failed: {type(e).__name__}: {e}"}

