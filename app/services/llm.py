import os
from typing import Dict, Any, Tuple

from app.schemas import GenerationMeta, TokenUsage
from app.llm.openai_client import (
    build_client,
    get_openai_model,
    get_fallback_policy,
    get_openai_key,
    OpenAIConfigError,
)

def llm_financial_summary(payload: Dict[str, Any], *, local_only: bool = False) -> Tuple[Dict[str, str], GenerationMeta]:
    """
    Build a concise, numbers-supported Summary / Financial Analysis / Financial Insights
    strictly as plain text (no JSON, no markdown tables, no UI hints).
    The model must cite quantities/totals that appear in the extracted data when possible.
    """
    lines = payload.get("lines", [])
    vendors = payload.get("vendors", [])
    totals = payload.get("totals", {})
    raw_text = payload.get("raw_text", "")[:15000]  # keep prompt bounded

    # Lightweight guard: if key not present, keep empty — UI will still render
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

    if not local_only:
        try:  # pragma: no cover - network call
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
        except Exception:
            text = ""
            meta = GenerationMeta(llm_used=False, forced_local=False)
    else:
        text = ""
        meta = GenerationMeta(llm_used=False, forced_local=True)

    if not text:
        # Fallback when the OpenAI client isn't configured or errors out. We still
        # want to provide the caller with something meaningful so the UI can render
        # useful text instead of empty strings.
        import re

        text = raw_text.strip()
        if not text:
            # Nothing could be extracted from the file; surface explicit placeholders
            return (
                {
                    "summary_text": "No textual content could be extracted from the document.",
                    "analysis_text": "No numeric data found for analysis.",
                    "insights_text": "No financial insights identified.",
                    "source": "local",
                },
                meta,
            )

        # crude summary: first 40 words from the raw text
        words = re.findall(r"\w+", text)
        summary = " ".join(words[:40]) if words else text[:200]

        # attempt to extract numeric values for a rudimentary analysis
        nums = [
            float(n.replace(",", ""))
            for n in re.findall(r"[-+]?[0-9,]*\.?[0-9]+", text)
        ]
        if nums:
            total = sum(nums)
            avg = total / len(nums)
            analysis = (
                f"The document references {len(nums)} numeric values totalling"
                f" approximately {total:.2f} with an average of {avg:.2f}."
            )
            insights = "High or unusual figures may warrant further review."
        else:
            analysis = "No numeric data found for analysis."
            insights = "No financial insights identified."

        return (
            {
                "summary_text": summary,
                "analysis_text": analysis,
                "insights_text": insights,
                "source": "local",
            },
            meta,
        )

    # Very light splitter: try to split into 3 blocks; if not, put everything in 'summary_text'
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    out = {"summary_text": text, "analysis_text": "", "insights_text": "", "source": "llm"}
    if len(blocks) >= 3:
        out = {
            "summary_text": blocks[0],
            "analysis_text": blocks[1],
            "insights_text": "\n\n".join(blocks[2:]),
        }
    return out, meta


def llm_financial_summary_file(filename: str, data: bytes, *, local_only: bool = False) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """Send a raw uploaded file to ChatGPT for three text sections with fallback.

    Local mode is used only when explicitly requested or when the configured
    fallback policy allows it.
    """

    def _local_summary() -> Tuple[Dict[str, str], Dict[str, Any]]:
        from app.utils.file_to_text import file_bytes_to_text

        raw_text = file_bytes_to_text(filename, data)
        out, _ = llm_financial_summary({"raw_text": raw_text}, local_only=True)
        meta_local = {
            "provider": "local",
            "model": None,
            "llm_used": "local",
            "fallback_reason": meta.get("fallback_reason", "none"),
        }
        return out, meta_local

    meta: Dict[str, Any] = {
        "provider": None,
        "model": None,
        "llm_used": None,
        "fallback_reason": "none",
    }

    if local_only:
        return _local_summary()

    policy = get_fallback_policy()
    key = get_openai_key()
    if not key:
        if policy in {"if_no_key", "on_error"}:
            meta["fallback_reason"] = "no_api_key"
            return _local_summary()
        raise OpenAIConfigError("Missing OpenAI API key")

    try:  # pragma: no cover - network call
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
        usage = getattr(resp, "usage", None)
        meta.update(
            {
                "provider": "openai",
                "model": model,
                "llm_used": "openai",
                "token_usage": {
                    "prompt_tokens": getattr(usage, "input_tokens", None),
                    "completion_tokens": getattr(usage, "output_tokens", None),
                    "total_tokens": getattr(usage, "total_tokens", None),
                },
            }
        )
    except Exception as e:
        if policy in {"on_error"}:
            meta["fallback_reason"] = f"openai_error:{type(e).__name__}"
            return _local_summary()
        raise

    if not text:
        meta["fallback_reason"] = "openai_error:empty_response"
        return _local_summary()

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
