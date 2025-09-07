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
    """Analyze a single file via the single-file orchestrator.

    The heavy lifting is done synchronously; this wrapper delegates to a
    background thread so the async callers remain non-blocking.
    """
    res = await asyncio.to_thread(process_single_file, name, data, local_only=local_only)
    meta = GenerationMeta(**res.pop("_meta", {}))
    return res, meta
