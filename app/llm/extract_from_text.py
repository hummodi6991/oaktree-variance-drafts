"""
LLM-assisted extractor that is *fact-bound*. It receives text chunks and asks the model
to return STRICT JSON matching a schema. Temperature=0; the prompt forbids invention.
Only used as a fallback when deterministic parsing is incomplete.
"""
from typing import List, Dict, Any
import os
import json
from openai import OpenAI

_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=_api_key) if _api_key else None

SYSTEM = (
  "You extract JSON ONLY from the provided text. "
  "Do not add fields that are not literally present. "
  "If a value is missing, return null. Never guess."
)

def extract_items_via_llm(text: str) -> List[Dict[str, Any]]:
    if not text or not text.strip():
        return []
    prompt = f"""
    From the text below, extract an array of items with this JSON schema:

    [
      {{
        "co_id": string|null,
        "description": string|null,
        "qty": number|null,
        "unit_price_sar": number|null,
        "amount_sar": number|null
      }}
    ]

    Rules:
    - Use only values explicitly present in the text.
    - If an amount is shown per line, return it; else null.
    - If qty and unit price are both present, you may compute amount_sar.
    - If values are ambiguous, return null for those fields.
    - Output STRICT JSON, no commentary.

    TEXT:
    ---
    {text[:6000]}
    ---
    """
    if client is None:
        return []
    try:
        resp = client.responses.create(
          model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
          temperature=0,
          input=[{"role":"system","content":SYSTEM},{"role":"user","content":prompt}],
          response_format={"type":"json_object"}
        )
        try:
            raw = resp.output_text
            data = json.loads(raw)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
                return data["items"]
        except Exception:
            pass
    except Exception:
        return []
    return []
