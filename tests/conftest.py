import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import types
import pytest


@pytest.fixture
def dummy_llm(monkeypatch):
    """Provide a dummy OpenAI client for tests."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class DummyClient:
        class files:
            @staticmethod
            def create(file, purpose, filename):
                return types.SimpleNamespace(id="file123")

        class responses:
            @staticmethod
            def create(*args, **kwargs):
                return types.SimpleNamespace(
                    output_text="Summary\n\nAnalysis\n\nInsights",
                    usage=types.SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2),
                )

    monkeypatch.setattr("openai_client_helper.build_client", lambda: DummyClient())
    monkeypatch.setattr("app.llm.openai_client.build_client", lambda: DummyClient())
    monkeypatch.setattr("app.services.llm.build_client", lambda: DummyClient())
    monkeypatch.setattr("app.gpt_client.build_client", lambda: DummyClient())
    yield
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
