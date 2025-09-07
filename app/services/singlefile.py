from __future__ import annotations

from typing import Dict, Any

from app.services.llm import llm_financial_summary_file
from app.utils.retries import retry_call


def process_single_file(
    filename: str,
    data: bytes,
    *_,
    **__,
) -> Dict[str, Any]:
    """Send a single uploaded file directly to ChatGPT for analysis.

    The file is transmitted to the LLM without any local parsing. The model
    returns three plain-text sections: summary, financial analysis and financial
    insights. Metadata about the generation is returned under ``_meta``.
    """

    res, meta = retry_call(llm_financial_summary_file, filename, data)
    meta["forced_local"] = False
    res["model_family"] = "chatgpt"
    res["_meta"] = meta
    return res

