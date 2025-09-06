import asyncio
import logging
from fastapi import UploadFile, File, Form, Request

from app.main import app
from app.services.singlefile import process_single_file
from app.utils.local import is_local_only
from app.schemas import GenerationMeta


logger = logging.getLogger(__name__)


# --- Single Data File endpoint ---
@app.post("/single/generate")
async def single_generate(request: Request, file: UploadFile = File(...), bilingual: bool = Form(True)):
    """Return LLM-generated summary/analysis/insights for any single file upload."""
    data = await file.read()
    local_only = is_local_only(request)
    res, meta = await asyncio.to_thread(
        process_single_file, file.filename or "upload.bin", data, local_only=local_only
    )
    logger.info("single_generate llm_used=%s model=%s forced_local=%s", meta.llm_used, meta.model, meta.forced_local)
    return {"kind": "insights", **res, "_meta": meta.model_dump()}

