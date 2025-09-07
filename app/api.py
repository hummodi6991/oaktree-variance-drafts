import logging
import asyncio
from fastapi import UploadFile, File, Form

from app.main import app
from app.services.singlefile import process_single_file

logger = logging.getLogger(__name__)

# --- Single Data File endpoint ---
@app.post("/single/generate")
async def single_generate(
    file: UploadFile = File(...),
    bilingual: bool = Form(True),
):
    """Return LLM-generated summary/analysis/insights for any single file upload."""
    data = await file.read()
    res = await asyncio.to_thread(
        process_single_file, file.filename or "upload.bin", data
    )
    meta = res.pop("_meta", {})
    logger.info(
        "single_generate llm_used=%s model=%s",
        meta.get("llm_used"),
        meta.get("model"),
    )
    return {"kind": "insights", **res, "_meta": meta}
