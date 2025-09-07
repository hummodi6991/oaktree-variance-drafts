from pathlib import Path
from io import BytesIO
import re
import pandas as pd

# If this helper already exists in your repo, keep the existing import.
from openai_file_upload import upload_bytes_as_file
from typing import Dict, Any, Tuple

from app.schemas import GenerationMeta, TokenUsage
from app.llm.openai_client import (
    build_client,
    get_openai_model,
    get_openai_key,
    OpenAIConfigError,
)


def _strip_markdown_noise(s: str) -> str:
    """Remove headings, bullets, emphasis, quotes, and code fences; return plain text."""
    if not s:
        return s
    s = re.sub(r"`{1,3}", "", s)
    s = re.sub(r"^\s*```[\s\S]*?^\s*```", "", s, flags=re.MULTILINE)
    s = re.sub(r"^\s*#{1,6}\s*", "", s, flags=re.MULTILINE)        # headings
    s = re.sub(r"^\s*([*\-•]\s+)", "", s, flags=re.MULTILINE)      # bullets
    s = re.sub(r"^\s*\d+\.\s+", "", s, flags=re.MULTILINE)         # 1. 2. 3.
    s = s.replace("**", "").replace("__", "")                      # bold/italic
    s = re.sub(r"^\s*>\s*", "", s, flags=re.MULTILINE)             # quotes
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def _bytes_to_text_for_llm(filename: str, data: bytes) -> str:
    """Convert CSV/Excel/TXT to UTF-8 text snapshot for the LLM."""
    ext = Path(filename).suffix.lower()
    if ext in {".csv", ".tsv"}:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            import chardet
            enc = (chardet.detect(data) or {}).get("encoding") or "utf-8"
            return data.decode(enc, errors="ignore")
    if ext == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(BytesIO(data)) as pdf:
                pages = [page.extract_text() or "" for page in pdf.pages]
            return "\n".join(pages)
        except Exception:
            return ""
    if ext in {".xlsx", ".xls"}:
        df = pd.read_excel(BytesIO(data), sheet_name=0)
        return df.to_csv(index=False)
    if ext in {".txt", ".md"}:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            import chardet
            enc = (chardet.detect(data) or {}).get("encoding") or "utf-8"
            return data.decode(enc, errors="ignore")
    # Fallback best-effort
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        import chardet
        enc = (chardet.detect(data) or {}).get("encoding") or "utf-8"
        return data.decode(enc, errors="ignore")


def llm_financial_summary(payload: Dict[str, Any]) -> Tuple[Dict[str, str], GenerationMeta]:
    """Return summary, analysis and insights using the OpenAI API only."""
    if not get_openai_key():
        raise OpenAIConfigError("Missing OpenAI API key")

    lines = payload.get("lines", [])
    vendors = payload.get("vendors", [])
    totals = payload.get("totals", {})
    raw_text = payload.get("raw_text", "")[:15000]

    prompt = f'''
You are a financial analyst. The user uploaded a SINGLE FILE with NO budget/actual pairs.
Return ONLY three plain-text sections in this exact order and nothing else:
1) Summary
2) Financial analysis
3) Financial insights

Rules:
- Use short paragraphs and bullets only when helpful.
- Support claims with NUMBERS found in the data when possible.
- Do NOT output any JSON, code blocks, tables, UI labels, 'diagnostics', or extra headers.
- No preambles or epilogues — only the three sections.

Extracted structured data (use when helpful):
- totals: {totals}
- vendors: {vendors}
- lines (first 40): {lines[:40]}

Raw text (possibly noisy, use prudently):
"""{raw_text}"""
'''

    client = build_client()
    msg = client.responses.create(
        model=get_openai_model(),
        temperature=0.2,
        input=[
            {"role": "system", "content": "Be precise, numeric, and concise. Output plain text only."},
            {"role": "user", "content": prompt},
        ],
    )
    text = _strip_markdown_noise((msg.output_text or "").strip())
    if not text:
        raise RuntimeError("Empty response from OpenAI")
    usage = getattr(msg, "usage", None)
    meta = GenerationMeta(
        llm_used=True,
        provider="OpenAI",
        model=get_openai_model(),
        token_usage=TokenUsage(
            prompt_tokens=getattr(usage, "input_tokens", None),
            completion_tokens=getattr(usage, "output_tokens", None),
            total_tokens=getattr(usage, "total_tokens", None),
        ),
        forced_local=False,
    )
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    out = {"summary_text": text, "analysis_text": "", "insights_text": "", "source": "llm"}
    if len(blocks) >= 3:
        out = {
            "summary_text": blocks[0],
            "analysis_text": blocks[1],
            "insights_text": "\n\n".join(blocks[2:]),
            "source": "llm",
        }
    return out, meta


def llm_financial_summary_file(filename: str, data: bytes) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """Accept PDF/CSV/Excel/TXT for single-file track and return 3 cleaned sections."""
    if not get_openai_key():
        raise OpenAIConfigError("Missing OpenAI API key")

    client = build_client()
    model = get_openai_model()
    ext = Path(filename).suffix.lower()

    instruction = (
        "Review the attached document and return three plain-text sections: "
        "Summary, Financial analysis, Financial insights."
    )

    if ext == ".pdf":
        try:
            file_id = upload_bytes_as_file(data, filename)
            user_content = [
                {"type": "input_text", "text": instruction},
                {"type": "input_file", "file_id": file_id},
            ]
        except Exception:
            text_blob = _bytes_to_text_for_llm(filename, data)[:18000]
            user_content = [
                {"type": "input_text", "text": instruction},
                {"type": "input_text", "text": f"FILE_NAME: {filename}\n\n{text_blob}"},
            ]
    else:
        text_blob = _bytes_to_text_for_llm(filename, data)[:18000]
        user_content = [
            {"type": "input_text", "text": instruction},
            {"type": "input_text", "text": f"FILE_NAME: {filename}\n\n{text_blob}"},
        ]

    msg = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": "Be precise, numeric, and concise. Output plain text only."},
            {"role": "user", "content": user_content},
        ],
    )
    text = _strip_markdown_noise((msg.output_text or "").strip())
    if not text:
        raise RuntimeError("Empty response from OpenAI")
    usage = getattr(msg, "usage", None)
    meta: Dict[str, Any] = {
        "provider": "openai",
        "model": model,
        "llm_used": "openai",
        "token_usage": {
            "prompt_tokens": getattr(usage, "input_tokens", None),
            "completion_tokens": getattr(usage, "output_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
        },
        "forced_local": False,
    }
    out = {"summary_text": text, "analysis_text": "", "insights_text": "", "source": "llm"}
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    if len(blocks) >= 3:
        out = {
            "summary_text": _strip_markdown_noise(blocks[0]),
            "analysis_text": _strip_markdown_noise(blocks[1]),
            "insights_text": _strip_markdown_noise("\n\n".join(blocks[2:])),
            "source": "llm",
        }
    return out, meta
