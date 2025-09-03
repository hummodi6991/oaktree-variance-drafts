from fastapi import APIRouter, UploadFile, File
from app.parsers.procurement_pdf import parse_procurement_pdf
from app.services.singlefile import process_single_file

router = APIRouter()

@router.post("/drafts/from-file")
async def from_file(file: UploadFile = File(...)):
    try:
        data = await file.read()
        is_pdf = file.filename.lower().endswith(".pdf")
        if is_pdf:
            result = parse_procurement_pdf(data)
            payload = {"procurement_summary": {"items": result.get("items", [])}, "meta": result.get("meta", {})}
        else:
            payload = process_single_file(file.filename, data)
        return payload
    except Exception as e:
        # Never bubble up to a 502 — surface as a structured response the UI can display.
        return {"error": f"single-file parse failed: {type(e).__name__}: {e}"}
