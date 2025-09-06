from typing import Dict, Any, Tuple
import asyncio

from app.services.singlefile import process_single_file
from app.schemas import GenerationMeta


async def analyze_single_file(
    data: bytes,
    name: str,
    bilingual: bool = True,
    no_speculation: bool = True,
    *,
    local_only: bool = False,
) -> Tuple[Dict[str, Any], GenerationMeta]:
    """Analyze a single file by delegating to ChatGPT for insights.

    ``process_single_file`` sends the raw file to the OpenAI API.  The network
    call is synchronous, so when invoked from an async FastAPI endpoint we
    offload the work to a thread via :func:`asyncio.to_thread` to avoid blocking
    the event loop.
    """
    res, meta = await asyncio.to_thread(process_single_file, name, data, local_only=local_only)
    return {"report_type": "summary", **res, "source": name}, meta
