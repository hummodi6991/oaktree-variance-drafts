import os
from openai import OpenAI

class OpenAIConfigError(RuntimeError):
    pass

def get_openai_model() -> str:
    return os.getenv("OPENAI_MODEL", "gpt-4o-mini")

def get_openai_base_url():
    return os.getenv("OPENAI_BASE_URL") or None

def get_openai_key():
    return os.getenv("OPENAI_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")

def get_fallback_policy() -> str:
    policy = os.getenv("LOCAL_FALLBACK_POLICY")
    if not policy:
        policy = "on_error" if os.getenv("ENV", "dev") != "prod" else "never"
    return policy

def ensure_openai_available():
    key = get_openai_key()
    if not key:
        raise OpenAIConfigError("Missing OpenAI API key")
    return key

def build_client() -> OpenAI:
    key = ensure_openai_available()
    base = get_openai_base_url()
    timeout = int(os.getenv("OPENAI_TIMEOUT", "30"))
    retries = int(os.getenv("OPENAI_MAX_RETRIES", "2"))
    params = {"api_key": key, "timeout": timeout, "max_retries": retries}
    if base:
        params["base_url"] = base
    return OpenAI(**params)
