
import os
import json
from typing import Tuple, Dict, Any

from .schemas import VarianceItem, ConfigModel, GenerationMeta, TokenUsage
from .prompt_contract import (
    SYSTEM_PROMPT,
    build_user_prompt,
    build_arabic_instruction,
)


def _fallback_text(v: VarianceItem, cfg: ConfigModel) -> Tuple[str, str]:
    """Return deterministic drafts when the OpenAI API is unavailable."""
    en = (
        f"{v.variance_pct:.2f}% (SAR {abs(v.variance_sar):,.0f}) variance in {v.category}. "
        f"{'Drivers: ' + '; '.join(v.drivers) + '. ' if v.drivers else 'Cause pending analyst review. '}"
        f"{'Vendors: ' + '; '.join(v.vendors) + '. ' if v.vendors else ''}"
        "Impact remains contained within management oversight; corrective actions are in progress if required."
    )
    ar = (
        f"تفاوت بنسبة {v.variance_pct:.2f}% (بقيمة {abs(v.variance_sar):,.0f} ريال) في فئة {v.category}. "
        f"{'الأسباب: ' + '؛ '.join(v.drivers) + '. ' if v.drivers else 'السبب قيد المراجعة من قبل المحلل. '}"
        f"{'الموردون: ' + '؛ '.join(v.vendors) + '. ' if v.vendors else ''}"
        "يبقى الأثر ضمن نطاق المتابعة الإدارية، وسيتم اتخاذ الإجراءات التصحيحية عند الحاجة."
    ) if cfg.bilingual else ""
    return en, ar


def generate_draft(v: VarianceItem, cfg: ConfigModel, *, local_only: bool = False) -> Tuple[str, str, GenerationMeta]:
    """Generate an evidence-based variance explanation using ChatGPT.

    The helper delegates to the OpenAI ChatGPT API when an API key is
    available. Prompts forbid speculation so returned drafts remain
    grounded in the provided `VarianceItem` fields.
    """
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    user_prompt = build_user_prompt(v, cfg)
    ar_instr = build_arabic_instruction() if cfg.bilingual else ""

    if local_only or not api_key:
        en, ar = _fallback_text(v, cfg)
        return en, ar, GenerationMeta(llm_used=False, forced_local=local_only)

    try:  # pragma: no cover - network call
        from openai import OpenAI

        timeout = int(os.getenv("OPENAI_TIMEOUT", "30"))
        retries = int(os.getenv("OPENAI_MAX_RETRIES", "2"))
        model = os.getenv("OPENAI_MODEL", "gpt-5.1-mini")
        client = OpenAI(api_key=api_key, timeout=timeout, max_retries=retries)
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt + ("\n\n" + ar_instr if ar_instr else "")},
        ]
        resp = client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
            timeout=timeout,
        )
        text = (resp.choices[0].message.content or "").strip()
        usage = getattr(resp, "usage", None)
        tu = TokenUsage(
            prompt_tokens=getattr(usage, "prompt_tokens", None),
            completion_tokens=getattr(usage, "completion_tokens", None),
            total_tokens=getattr(usage, "total_tokens", None),
        )
        meta = GenerationMeta(
            llm_used=True,
            provider="OpenAI",
            model=model,
            token_usage=tu,
            forced_local=False,
        )
        if cfg.bilingual and "\n\n" in text:
            en, ar = text.split("\n\n", 1)
            return en.strip(), ar.strip(), meta
        return text, "", meta
    except Exception:
        en, ar = _fallback_text(v, cfg)
        return en, ar, GenerationMeta(llm_used=False, forced_local=False)


def summarize_financials(summary: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    """Summarize procurement data using ChatGPT with a deterministic fallback."""
    highlights: list[str] = []
    if isinstance(summary, dict):
        highlights.extend(summary.get("highlights", []))
    if isinstance(analysis, dict):
        highlights.extend(analysis.get("highlights", []))
    fallback = " ".join(highlights).strip() or "No financial insights available."

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return fallback
    try:
        from openai import OpenAI

        timeout = int(os.getenv("OPENAI_TIMEOUT", "30"))
        retries = int(os.getenv("OPENAI_MAX_RETRIES", "2"))
        client = OpenAI(api_key=api_key, timeout=timeout, max_retries=retries)
        prompt = (
            "You are a financial analyst. Using the data below, write a concise "
            "summary highlighting key financial insights.\n\n"
            f"Highlights: {', '.join(highlights)}\n"
            f"Analysis: {json.dumps(analysis, default=str)[:4000]}"
        )
        messages = [
            {"role": "system", "content": "You are a helpful financial analysis assistant."},
            {"role": "user", "content": prompt},
        ]
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-5.1-mini"),
            messages=messages,  # type: ignore[arg-type]
            timeout=timeout,
        )
        text = (resp.choices[0].message.content or "").strip()
        return text or fallback
    except Exception:
        return fallback
