import types
import pytest
from fastapi.testclient import TestClient

from app.main import app, require_api_key
from app.services.singlefile import process_single_file
from app.llm.openai_client import OpenAIConfigError

app.dependency_overrides[require_api_key] = lambda: None

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


def test_local_only_flag_ignored(dummy_llm):
    client = TestClient(app)
    files = {"file": ("note.txt", b"hi", "text/plain")}
    r = client.post("/drafts/from-file", files=files, data={"local_only": "true"})
    assert r.status_code == 200
    meta = r.json()["_meta"]
    assert meta["llm_used"] == "openai"


def test_headers_ignored_for_local_only(dummy_llm):
    client = TestClient(app)
    files = {"file": ("note.txt", b"hi", "text/plain")}
    r = client.post("/drafts/from-file", files=files, headers={"x-local-only": "true"})
    assert r.status_code == 200
    meta = r.json()["_meta"]
    assert meta["llm_used"] == "openai"


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(OpenAIConfigError):
        process_single_file("note.txt", b"hi")


def test_openai_error_propagates(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class BoomClient(DummyClient):
        class responses:
            @staticmethod
            def create(*args, **kwargs):
                raise RuntimeError("boom")

    monkeypatch.setattr("app.llm.openai_client.build_client", lambda: BoomClient())
    monkeypatch.setattr("app.services.llm.build_client", lambda: BoomClient())
    with pytest.raises(RuntimeError):
        process_single_file("note.txt", b"hi")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
