from fastapi import APIRouter, UploadFile, File
import asyncio
from app.parsers.procurement_pdf import parse_procurement_pdf
from app.services.singlefile import process_single_file

router = APIRouter()

@router.post("/drafts/from-file")
async def from_file(file: UploadFile = File(...)):
    try:
        data = await file.read()
        is_pdf = file.filename.lower().endswith(".pdf")
        if is_pdf:
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(parse_procurement_pdf, data),
                    timeout=25,
                )
            except asyncio.TimeoutError:
                return {"error": "PDF parsing timed out after 25 seconds."}
            return {
                "procurement_summary": {"items": result.get("items", [])},
                "meta": result.get("meta", {})
            }
        processed = await asyncio.to_thread(process_single_file, file.filename, data)
        return processed
    except Exception as e:
        # Surface as JSON the UI can show instead of crashing upstream (502).
        return {"error": f"single-file parse failed: {type(e).__name__}: {e}"}
