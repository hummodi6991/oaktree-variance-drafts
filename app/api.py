from fastapi import UploadFile, File, Form

from app.main import app
from app.services.singlefile import process_single_file


# --- Single Data File endpoint ---
@app.post("/single/generate")
async def single_generate(file: UploadFile = File(...), bilingual: bool = Form(True)):
    """Return LLM-generated summary/analysis/insights for any single file upload."""
    b = await file.read()
    res = process_single_file(file.filename, b)
    return {"kind": "insights", **res}

