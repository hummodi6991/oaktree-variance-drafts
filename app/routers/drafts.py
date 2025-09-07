from fastapi import APIRouter, UploadFile, File, Request
import asyncio
import logging

from app.services.singlefile import process_single_file
from app.utils.local import is_local_only

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/drafts/from-file")
async def from_file(request: Request, file: UploadFile = File(...)):
    """Simplified single-file endpoint delegating work to ChatGPT.

    The uploaded file is forwarded to the LLM without any local parsing.  The
    model responds with three plain-text sections: summary, financial analysis
    and financial insights.
    """
    try:
        data = await file.read()
        local_only = is_local_only(request)
        res = await asyncio.to_thread(
            process_single_file, file.filename, data, local_only=local_only
        )
        meta = res.pop("_meta", {})
        logger.info(
            "drafts/from-file llm_used=%s model=%s forced_local=%s",
            meta.get("llm_used"),
            meta.get("model"),
            meta.get("forced_local"),
        )
        return {"kind": "insights", **res, "_meta": meta}
    except Exception as e:  # pragma: no cover - defensive
        return {"error": str(e)}
