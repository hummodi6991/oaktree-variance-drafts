from __future__ import annotations

from typing import Dict, Any, Tuple

from app.services.llm import llm_financial_summary_file, llm_financial_summary
from app.schemas import GenerationMeta
import os

FORCE_LLM = os.getenv("FORCE_LLM", "1") in ("1", "true", "TRUE", "yes", "YES")


def process_single_file(
    filename: str,
    data: bytes,
    *_,
    local_only: bool = False,
    **__,
) -> Tuple[Dict[str, Any], GenerationMeta]:
    """Send a single uploaded file directly to ChatGPT for analysis.

    The file is transmitted to the LLM without any local parsing.  The model
    returns three plain-text sections: summary, financial analysis and financial
    insights.  When the OpenAI API is unavailable, a deterministic text-based
    fallback is used so the UI still renders meaningful output during testing.
    """

    try:
        return llm_financial_summary_file(filename, data, local_only=local_only)
    except Exception:
        if FORCE_LLM and not local_only:
            # Force LLM-assist: surface the error instead of silently falling back
            raise
        # Allow legacy/local fallback when FORCE_LLM is disabled
        from app.utils.file_to_text import file_bytes_to_text

        text = file_bytes_to_text(filename, data)
        return llm_financial_summary({"raw_text": text}, local_only=True)

# (Note: llm_financial_summary_file already stamps source='llm' on success and 'local' on fallback)
# see the existing 'source' assignments in this function.
