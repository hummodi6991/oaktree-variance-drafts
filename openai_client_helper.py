import os
from openai import OpenAI

def build_client():
    base = os.getenv("OPENAI_BASE_URL") or None
    api_key = os.getenv("OPENAI_API_KEY") or None
    timeout = int(os.getenv("OPENAI_TIMEOUT", "30"))
    retries = int(os.getenv("OPENAI_MAX_RETRIES", "2"))
    params = {"timeout": timeout, "max_retries": retries}
    if base:
        params["base_url"] = base
    if api_key:
        params["api_key"] = api_key
    return OpenAI(**params)
