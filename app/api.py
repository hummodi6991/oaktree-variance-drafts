import asyncio
import logging
from fastapi import UploadFile, File, Form, Request

from app.main import app
from app.services.singlefile import process_single_file
from app.utils.local import is_local_only


logger = logging.getLogger(__name__)


# --- Single Data File endpoint ---
@app.post("/single/generate")
async def single_generate(
    request: Request,
    file: UploadFile = File(...),
    bilingual: bool = Form(True),
    local_only: bool = Form(False),
    localOnly: bool = Form(False),
):
    """Return LLM-generated summary/analysis/insights for any single file upload."""
    data = await file.read()
    force_local = local_only or localOnly or is_local_only(request)
    res = await asyncio.to_thread(
        process_single_file, file.filename or "upload.bin", data, local_only=force_local
    )
    meta = res.pop("_meta", {})
    logger.info(
        "single_generate llm_used=%s model=%s forced_local=%s",
        meta.get("llm_used"),
        meta.get("model"),
        meta.get("forced_local"),
    )
    return {"kind": "insights", **res, "_meta": meta}

