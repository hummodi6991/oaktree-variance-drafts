from __future__ import annotations

from typing import Dict, Any

from app.services.llm import llm_financial_summary_file
from app.utils.retries import retry_call


def process_single_file(
    filename: str,
    data: bytes,
    *_,
    local_only: bool = False,
    **__,
) -> Dict[str, Any]:
    """Send a single uploaded file directly to ChatGPT for analysis.

    The file is transmitted to the LLM without any local parsing. The model
    returns three plain-text sections: summary, financial analysis and financial
    insights.  ``GenerationMeta`` is embedded into the returned dict under
    ``"_meta"``.
    """

    # LLM-only: use retries for transient failures then surface the result.
    res, meta = retry_call(
        llm_financial_summary_file, filename, data, local_only=local_only
    )
    res["_meta"] = meta.model_dump()
    return res

