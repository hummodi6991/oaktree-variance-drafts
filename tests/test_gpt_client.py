import os
from app.gpt_client import generate_draft
from app.schemas import VarianceItem, ConfigModel

class DummyOpenAI:
    last_kwargs = None

    def __init__(self, api_key: str, timeout: int, max_retries: int):
        DummyOpenAI.last_kwargs = {
            "api_key": api_key,
            "timeout": timeout,
            "max_retries": max_retries,
        }
        class _Chat:
            class _Completions:
                def create(self, *args, **kwargs):
                    raise TimeoutError("boom")
            completions = _Completions()
        self.chat = _Chat()

def test_generate_draft_timeout(monkeypatch):
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_TIMEOUT"] = "1"
    monkeypatch.setattr("openai.OpenAI", DummyOpenAI)

    v = VarianceItem(
        project_id="P1",
        period="2024-01",
        category="Materials",
        budget_sar=1000.0,
        actual_sar=1200.0,
        variance_sar=200.0,
        variance_pct=20.0,
        drivers=["CO-1"],
        vendors=["Vendor"],
    )
    cfg = ConfigModel()
    en, ar = generate_draft(v, cfg)
    assert "variance" in en
    assert DummyOpenAI.last_kwargs["timeout"] == 1
    assert DummyOpenAI.last_kwargs["max_retries"] == 0
    assert ar  # bilingual fallback text
    os.environ.pop("OPENAI_API_KEY")
    os.environ.pop("OPENAI_TIMEOUT")
