import os
import os
import json
from typing import Tuple, Dict, Any

from .schemas import VarianceItem, ConfigModel, GenerationMeta, TokenUsage
from .prompt_contract import (
    SYSTEM_PROMPT,
    build_user_prompt,
    build_arabic_instruction,
)
from openai_client_helper import build_client
from app.llm.openai_client import OpenAIConfigError


def generate_draft(v: VarianceItem, cfg: ConfigModel) -> Tuple[str, str, GenerationMeta]:
    """Generate an evidence-based variance explanation using ChatGPT.

    Drafts are always produced by the LLM; deterministic local fallbacks have
    been removed. An ``OpenAIConfigError`` is raised if no API key is
    configured.
    """
    api_key = (
        os.getenv("OPENAI_API_KEY")
        or os.getenv("AZURE_OPENAI_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise OpenAIConfigError("Missing OpenAI API key")

    user_prompt = build_user_prompt(v, cfg)
    ar_instr = build_arabic_instruction() if cfg.bilingual else ""

    timeout = int(os.getenv("OPENAI_TIMEOUT", "30"))
    model = os.getenv("OPENAI_MODEL", "gpt-5.1-mini")
    client = build_client()
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt + ("\n\n" + ar_instr if ar_instr else "")},
    ]
    resp = client.responses.create(
        model=model,
        input=messages,
        timeout=timeout,
    )
    text = (resp.output_text or "").strip()
    if not text:
        raise RuntimeError("Empty response from OpenAI")
    usage = getattr(resp, "usage", None)
    tu = TokenUsage(
        prompt_tokens=getattr(usage, "input_tokens", None),
        completion_tokens=getattr(usage, "output_tokens", None),
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


def summarize_financials(summary: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    """Summarize procurement data using the OpenAI API without fallbacks."""
    highlights: list[str] = []
    if isinstance(summary, dict):
        highlights.extend(summary.get("highlights", []))
    if isinstance(analysis, dict):
        highlights.extend(analysis.get("highlights", []))

    api_key = (
        os.getenv("OPENAI_API_KEY")
        or os.getenv("AZURE_OPENAI_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise OpenAIConfigError("Missing OpenAI API key")

    timeout = int(os.getenv("OPENAI_TIMEOUT", "30"))
    client = build_client()
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
    resp = client.responses.create(
        model=os.getenv("OPENAI_MODEL", "gpt-5.1-mini"),
        input=messages,
        timeout=timeout,
    )
    text = (resp.output_text or "").strip()
    if not text:
        raise RuntimeError("Empty response from OpenAI")
    return text
