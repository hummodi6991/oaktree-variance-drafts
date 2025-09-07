import os
import pytest
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
    with pytest.raises(TimeoutError):
        generate_draft(v, cfg)
    assert DummyOpenAI.last_kwargs["timeout"] == 1
    assert DummyOpenAI.last_kwargs["max_retries"] == 5
    os.environ.pop("OPENAI_API_KEY")
    os.environ.pop("OPENAI_TIMEOUT")
    os.environ.pop("OPENAI_MAX_RETRIES")


class BilingualDummyClient:
    class _Resp:
        output_text = "English line 1\nEnglish line 2\n\nالشرح العربي هنا"
        usage = None

    class _Responses:
        def create(self, *args, **kwargs):
            return BilingualDummyClient._Resp()

    def __init__(self, *args, **kwargs):
        self.responses = BilingualDummyClient._Responses()


def test_generate_draft_bilingual_split(monkeypatch):
    os.environ["OPENAI_API_KEY"] = "sk-test"
    monkeypatch.setattr("app.gpt_client.build_client", lambda: BilingualDummyClient())

    v = VarianceItem(
        project_id="P1",
        period="2024-01",
        category="Materials",
        budget_sar=1000.0,
        actual_sar=1200.0,
        variance_sar=200.0,
        variance_pct=20.0,
    )
    cfg = ConfigModel(bilingual=True)
    en, ar, _ = generate_draft(v, cfg)
    assert en == "English line 1\nEnglish line 2"
    assert ar == "الشرح العربي هنا"
    os.environ.pop("OPENAI_API_KEY")
