from fastapi import APIRouter, UploadFile, File, Form
import asyncio
import logging

from app.services.singlefile import process_single_file

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/drafts/from-file")
async def from_file(
    file: UploadFile = File(...),
    local_only: bool = Form(False),
    localOnly: bool = Form(False),
):
    """Simplified single-file endpoint delegating work to ChatGPT.

    The uploaded file is forwarded to the LLM without any local parsing.  The
    model responds with three plain-text sections: summary, financial analysis
    and financial insights.
    """
    try:
        data = await file.read()
        # Only honor explicit body flags for local mode; ignore headers/query params
        force_local = bool(local_only or localOnly)
        res = await asyncio.to_thread(
            process_single_file, file.filename, data, local_only=force_local
        )
        meta = res.pop("_meta", {})
        logger.info(
            "drafts/from-file llm_used=%s model=%s forced_local=%s fallback_reason=%s",
            meta.get("llm_used"),
            meta.get("model"),
            meta.get("forced_local"),
            meta.get("fallback_reason"),
        )
        return {"kind": "insights", **res, "_meta": meta}
    except Exception as e:  # pragma: no cover - defensive
        return {"error": str(e)}
