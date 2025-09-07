from typing import Dict, Any, Tuple

from app.schemas import GenerationMeta, TokenUsage
from app.llm.openai_client import (
    build_client,
    get_openai_model,
    get_openai_key,
    OpenAIConfigError,
)


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
    text = (msg.output_text or "").strip()
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
    """Send a raw uploaded file to ChatGPT for three text sections."""
    if not get_openai_key():
        raise OpenAIConfigError("Missing OpenAI API key")

    client = build_client()
    model = get_openai_model()
    upload = client.files.create(file=data, purpose="assistants", filename=filename)
    resp = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": "Be precise, numeric, and concise. Output plain text only.",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "Review the attached document and return three plain-text "
                            "sections: Summary, Financial analysis, Financial insights."
                        ),
                    },
                    {"type": "input_file", "file_id": upload.id},
                ],
            },
        ],
    )
    text = (resp.output_text or "").strip()
    if not text:
        raise RuntimeError("Empty response from OpenAI")
    usage = getattr(resp, "usage", None)
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
