from fastapi import APIRouter, UploadFile, File
import asyncio

from app.services.singlefile import process_single_file

router = APIRouter()


@router.post("/drafts/from-file")
async def from_file(file: UploadFile = File(...)):
    """Simplified single-file endpoint delegating work to ChatGPT.

    The uploaded file is converted to plain text and sent to the LLM which
    returns three textual sections: summary, financial analysis and financial
    insights.
    """
    try:
        data = await file.read()
        res = await asyncio.to_thread(process_single_file, file.filename, data)
        return {"kind": "insights", **res}
    except Exception as e:  # pragma: no cover - defensive
        return {"error": str(e)}
