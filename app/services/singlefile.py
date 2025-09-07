from __future__ import annotations

from typing import Dict, Any

from app.services.llm import llm_financial_summary_file
from app.utils.retries import retry_call
from app.config import FORCE_LLM


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
    insights. Metadata about the generation is returned under ``_meta``.
    """

    # LLM-only: no fallbacks. Use retries for transient failures, then raise.
    effective_local = False if FORCE_LLM else bool(local_only)
    res, meta = retry_call(
        llm_financial_summary_file, filename, data, local_only=effective_local
    )
    meta["forced_local"] = effective_local
    res["model_family"] = "local" if effective_local else "chatgpt"
    res["_meta"] = meta
    return res

