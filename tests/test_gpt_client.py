import os
from app.gpt_client import generate_draft
from app.schemas import VarianceItem, ConfigModel

class DummyOpenAI:
    last_kwargs = None

    def __init__(self, api_key=None, timeout=None, max_retries=None, base_url=None):
        DummyOpenAI.last_kwargs = {
            "api_key": api_key,
            "timeout": timeout,
            "max_retries": max_retries,
        }
        class _Responses:
            def create(self, *args, **kwargs):
                raise TimeoutError("boom")
        self.responses = _Responses()

def test_generate_draft_timeout(monkeypatch):
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_TIMEOUT"] = "1"
    os.environ["OPENAI_MAX_RETRIES"] = "5"
    monkeypatch.setattr("openai_client_helper.OpenAI", DummyOpenAI)

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
    en, ar, meta = generate_draft(v, cfg)
    assert "variance" in en
    assert DummyOpenAI.last_kwargs["timeout"] == 1
    assert DummyOpenAI.last_kwargs["max_retries"] == 5
    assert ar  # bilingual fallback text
    assert meta.llm_used is False
    os.environ.pop("OPENAI_API_KEY")
    os.environ.pop("OPENAI_TIMEOUT")
    os.environ.pop("OPENAI_MAX_RETRIES")
