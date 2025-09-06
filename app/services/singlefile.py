from __future__ import annotations

from typing import Dict, Any

from app.utils.file_to_text import file_bytes_to_text
from app.services.llm import llm_financial_summary


def process_single_file(filename: str, data: bytes, *_, **__) -> Dict[str, Any]:
    """Route single-file uploads to the LLM for summary/analysis/insights.

    No local parsing of budget/actual data is performed. The file content is
    converted to plain text and handed to ChatGPT which returns three textual
    sections: summary, financial analysis and financial insights.
    """
    text = file_bytes_to_text(filename, data)
    llm_out = llm_financial_summary({"raw_text": text})
    return {
        "summary_text": llm_out.get("summary_text", ""),
        "analysis": {"text": llm_out.get("analysis_text", "")},
        "insights": {"text": llm_out.get("insights_text", "")},
    }
