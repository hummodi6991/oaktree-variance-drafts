from typing import Dict, Any
import asyncio

from app.services.singlefile import process_single_file


async def analyze_single_file(
    data: bytes,
    name: str,
    bilingual: bool = True,
    no_speculation: bool = True,
    *,
    local_only: bool = False,
) -> Dict[str, Any]:
    """Analyze a single file by delegating to ChatGPT for insights.

    ``process_single_file`` sends the raw file to the OpenAI API. The network
    call is synchronous, so when invoked from an async FastAPI endpoint we
    offload the work to a thread via :func:`asyncio.to_thread` to avoid blocking
    the event loop.  ``GenerationMeta`` is already embedded in the returned
    dictionary via ``"_meta"``.
    """
    res = await asyncio.to_thread(process_single_file, name, data, local_only=local_only)
    res.update({"report_type": "summary", "source": name})
    return res
