from __future__ import annotations

from typing import Dict, Any

from app.services.llm import llm_financial_summary_file, llm_financial_summary


def process_single_file(filename: str, data: bytes, *_, **__) -> Dict[str, Any]:
    """Send a single uploaded file directly to ChatGPT for analysis.

    The file is transmitted to the LLM without any local parsing.  The model
    returns three plain-text sections: summary, financial analysis and financial
    insights.  When the OpenAI API is unavailable, a deterministic text-based
    fallback is used so the UI still renders meaningful output during testing.
    """

    try:
        return llm_financial_summary_file(filename, data)
    except Exception:  # pragma: no cover - defensive fallback
        from app.utils.file_to_text import file_bytes_to_text

        text = file_bytes_to_text(filename, data)
        return llm_financial_summary({"raw_text": text})
