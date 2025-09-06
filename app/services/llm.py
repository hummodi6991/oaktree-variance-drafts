import os
from typing import Dict, Any

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def _openai_client():
    from openai import OpenAI  # requires openai>=1.0
    return OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def llm_financial_summary(payload: Dict[str, Any]) -> Dict[str, str]:
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

    try:
        client = _openai_client()
        msg = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "Be precise, numeric, and concise. Output plain text only."},
                {"role": "user", "content": prompt},
            ],
        )
        text = (msg.choices[0].message.content or "").strip()
        source = "llm"
    except Exception:
        # Fallback when the OpenAI client isn't configured or errors out.  We still
        # want to provide the caller with something meaningful so the UI can render
        # useful text instead of empty strings.  The fallback is intentionally
        # lightweight: it uses the raw text for a short summary and performs a very
        # small amount of numeric analysis if possible.
        import re

        text = raw_text.strip()
        if not text:
            # Nothing could be extracted from the file; surface explicit placeholders
            return {
                "summary_text": "No textual content could be extracted from the document.",
                "analysis_text": "No numeric data found for analysis.",
                "insights_text": "No financial insights identified.",
                "source": "local",
            }

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

        return {
            "summary_text": summary,
            "analysis_text": analysis,
            "insights_text": insights,
            "source": "local",
        }

    # Very light splitter: try to split into 3 blocks; if not, put everything in 'summary_text'
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    out = {"summary_text": text, "analysis_text": "", "insights_text": "", "source": source}
    if len(blocks) >= 3:
        out = {
            "summary_text": blocks[0],
            "analysis_text": blocks[1],
            "insights_text": "\n\n".join(blocks[2:]),
        }
    return out


def llm_financial_summary_file(filename: str, data: bytes) -> Dict[str, str]:
    """Send a raw uploaded file to ChatGPT for three text sections.

    The file is transmitted to the OpenAI API as an attachment so no local
    parsing is required.  When the API is unavailable or the request fails, the
    function falls back to converting the file to text and delegating to
    :func:`llm_financial_summary`.
    """

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        from app.utils.file_to_text import file_bytes_to_text

        text = file_bytes_to_text(filename, data)
        return llm_financial_summary({"raw_text": text})

    try:  # pragma: no cover - network call
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        upload = client.files.create(file=data, purpose="assistants", filename=filename)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
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
        text = (resp.choices[0].message.content or "").strip()
        source = "llm"
    except Exception:
        text = ""
        source = "local"

    if not text:
        from app.utils.file_to_text import file_bytes_to_text

        raw_text = file_bytes_to_text(filename, data)
        return llm_financial_summary({"raw_text": raw_text})

    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    out = {"summary_text": text, "analysis_text": "", "insights_text": "", "source": source}
    if len(blocks) >= 3:
        out = {
            "summary_text": blocks[0],
            "analysis_text": blocks[1],
            "insights_text": "\n\n".join(blocks[2:]),
            "source": source,
        }
    return out
