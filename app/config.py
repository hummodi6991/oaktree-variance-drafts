import os

FORCE_LLM = os.getenv("FORCE_LLM", "").lower() in {"1", "true", "yes"}
