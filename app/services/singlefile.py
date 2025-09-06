from __future__ import annotations

from typing import Dict, Any, Tuple

from app.services.llm import llm_financial_summary_file, llm_financial_summary
from app.schemas import GenerationMeta
import os
from app.utils.retries import retry_call

FORCE_LLM = os.getenv("FORCE_LLM", "1") in ("1", "true", "TRUE", "yes", "YES")


def process_single_file(
    filename: str,
    data: bytes,
    *_,
    local_only: bool = False,
    **__,
) -> Tuple[Dict[str, Any], GenerationMeta]:
    """Send a single uploaded file directly to ChatGPT for analysis.

    The file is transmitted to the LLM without any local parsing. The model
    returns three plain-text sections: summary, financial analysis and financial
    insights.
    """

    # LLM-only: no fallbacks. Use retries for transient failures, then raise.
    return retry_call(llm_financial_summary_file, filename, data, local_only=False)

