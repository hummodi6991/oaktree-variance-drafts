from typing import Dict, Any
import asyncio

from app.services.singlefile import process_single_file


async def analyze_single_file(
    data: bytes,
    name: str,
    bilingual: bool = True,
    no_speculation: bool = True,
) -> Dict[str, Any]:
    """Analyze a single file by delegating to ChatGPT for insights.

    ``process_single_file`` performs synchronous, potentially heavy operations such
    as PDF or spreadsheet parsing.  When called from an async FastAPI endpoint this
    would block the event loop, preventing other requests from being served.  To
    keep the single-file track responsive we offload the processing to a worker
    thread via :func:`asyncio.to_thread`.
    """
    res = await asyncio.to_thread(process_single_file, name, data)
    return {"report_type": "summary", **res, "source": name}
