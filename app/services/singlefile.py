from __future__ import annotations
from typing import Dict, Any, List, Optional
import os

from app.parsers.single_file_intake import parse_single_file
from app.llm.openai_client import build_client, get_openai_model
from app.utils.local import to_markdown_table

"""Single-file orchestrator:
1) Parse upload locally (detect variance or build procurement/financial items).
2) If variance_items exist -> return them + optional LLM commentary.
3) Else -> return financial summary/analysis/insights (local) and optionally ask LLM to enhance narrative.
Always set meta flags so the UI can disclose AI assistance.
"""

def _use_llm(local_only: bool) -> bool:
    if local_only:
        return False
    return bool(os.getenv("OPENAI_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY"))

def _summarize_variance_with_llm(rows: List[dict]) -> str:
    client = build_client()
    model = get_openai_model()
    content = (
        "You are a finance analyst. Briefly summarize key drivers of variance (top over/under) from the table of "
        "[label, budget_sar, actual_sar, variance_sar]. Be concise and factual."
    )
    table = to_markdown_table(rows[:200])  # safety cap
    resp = client.responses.create(
        model=model,
        temperature=0.2,
        input=[
            {"role": "system", "content": content},
            {"role": "user", "content": f"Variance table:\n\n{table}"},
        ],
    )
    return resp.output_text.strip()

def _summarize_financials_with_llm(summary: dict) -> dict:
    client = build_client()
    model = get_openai_model()
    prompt = (
        "You are a finance analyst. The app produced a financial summary/analysis/insights.\n"
        "Rewrite for clarity and executive brevity. Do not invent numbers; keep it grounded."
    )
    resp = client.responses.create(
        model=model,
        temperature=0.2,
        input=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": str(summary)[:12000]},
        ],
    )
    text = resp.output_text.strip()
    return {"narrative": text}

def process_single_file(filename: str, data: bytes, *, local_only: bool = False) -> Dict[str, Any]:
    meta = {
        "llm_used": False,
        "provider": None,
        "model": None,
        "forced_local": bool(local_only),
        "fallback_reason": None,
    }

    use_llm = _use_llm(local_only)
    if not use_llm and not local_only:
        meta["fallback_reason"] = "no_api_key"

    parsed = parse_single_file(filename, data)

    if "variance_items" in parsed and parsed["variance_items"]:
        out: Dict[str, Any] = {
            "report_type": "variance",
            "variance_items": parsed["variance_items"],
            "diagnostics": parsed.get("diagnostics", {}),
        }
        if use_llm:
            try:
                out["variance_commentary"] = _summarize_variance_with_llm(parsed["variance_items"])
                meta.update({
                    "llm_used": True,
                    "provider": "OpenAI",
                    "model": get_openai_model(),
                })
            except Exception as e:  # pragma: no cover - network failure
                meta["fallback_reason"] = f"variance_llm_error: {e}"
        return {**out, "_meta": meta}

    summary_like = {
        "procurement_summary": parsed.get("procurement_summary"),
        "analysis": parsed.get("analysis"),
        "economic_analysis": parsed.get("economic_analysis"),
        "insights": parsed.get("insights"),
        "diagnostics": parsed.get("diagnostics"),
    }
    out = {"report_type": "summary", **summary_like}

    if use_llm:
        try:
            enhanced = _summarize_financials_with_llm(summary_like)
            out["ai_narrative"] = enhanced["narrative"]
            meta.update({
                "llm_used": True,
                "provider": "OpenAI",
                "model": get_openai_model(),
            })
        except Exception as e:  # pragma: no cover - network failure
            meta["fallback_reason"] = f"summary_llm_error: {e}"

    out["_meta"] = meta
    return out
