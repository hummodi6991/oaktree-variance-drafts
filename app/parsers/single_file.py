from typing import Dict, Any

from app.services.singlefile import process_single_file


async def analyze_single_file(
    data: bytes,
    name: str,
    bilingual: bool = True,
    no_speculation: bool = True,
) -> Dict[str, Any]:
    """Analyze a single file by delegating to ChatGPT for insights."""
    res = process_single_file(name, data)
    return {"report_type": "summary", **res, "source": name}
