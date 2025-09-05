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
    except Exception:
        # Fallback if API is not configured; return empty and let caller fill from local summaries
        text = ""

    # Very light splitter: try to split into 3 blocks; if not, put everything in 'summary_text'
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    out = {"summary_text": text, "analysis_text": "", "insights_text": ""}
    if len(blocks) >= 3:
        out = {
            "summary_text": blocks[0],
            "analysis_text": blocks[1],
            "insights_text": "\n\n".join(blocks[2:]),
        }
    return out
