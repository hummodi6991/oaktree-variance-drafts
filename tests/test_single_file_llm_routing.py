from fastapi.testclient import TestClient
import types
import pytest

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
        def create(model, input):
            return types.SimpleNamespace(
                output_text="Summary\n\nAnalysis\n\nInsights",
                usage=types.SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2),
            )


def test_single_file_local_only_true_uses_local(monkeypatch):
    client = TestClient(app)
    files = {"file": ("note.txt", b"hi", "text/plain")}
    r = client.post("/drafts/from-file", files=files, data={"local_only": "true"})
    assert r.status_code == 200
    meta = r.json()["_meta"]
    assert meta["llm_used"] == "local"
    assert meta["provider"] == "local"
    assert meta["forced_local"] is True


def test_single_file_no_local_flag_uses_openai(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr("app.llm.openai_client.build_client", lambda: DummyClient())
    monkeypatch.setattr("app.services.llm.build_client", lambda: DummyClient())
    client = TestClient(app)
    files = {"file": ("note.txt", b"hi", "text/plain")}
    r = client.post("/drafts/from-file", files=files)
    assert r.status_code == 200
    meta = r.json()["_meta"]
    assert meta["llm_used"] == "openai"
    assert meta["provider"] == "openai"
    assert meta["model"] == "gpt-4o-mini"
    assert meta["forced_local"] is False
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)


def test_headers_ignored_for_local_only(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr("app.llm.openai_client.build_client", lambda: DummyClient())
    monkeypatch.setattr("app.services.llm.build_client", lambda: DummyClient())
    client = TestClient(app)
    files = {"file": ("note.txt", b"hi", "text/plain")}
    r = client.post("/drafts/from-file", files=files, headers={"x-local-only": "true"})
    assert r.status_code == 200
    meta = r.json()["_meta"]
    assert meta["llm_used"] == "openai"
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)


def test_force_llm_overrides_local_flag(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr("app.services.singlefile.FORCE_LLM", True, raising=False)
    monkeypatch.setattr("app.llm.openai_client.build_client", lambda: DummyClient())
    monkeypatch.setattr("app.services.llm.build_client", lambda: DummyClient())
    client = TestClient(app)
    files = {"file": ("note.txt", b"hi", "text/plain")}
    r = client.post("/drafts/from-file", files=files, data={"local_only": "true"})
    assert r.status_code == 200
    meta = r.json()["_meta"]
    assert meta["llm_used"] == "openai"
    assert meta["forced_local"] is False
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)


def test_fallback_policy_on_error_missing_key(monkeypatch):
    monkeypatch.setenv("LOCAL_FALLBACK_POLICY", "on_error")
    res = process_single_file("note.txt", b"hi")
    meta = res["_meta"]
    assert meta["llm_used"] == "local"
    assert meta["fallback_reason"] == "no_api_key"
    monkeypatch.delenv("LOCAL_FALLBACK_POLICY", raising=False)


def test_fallback_policy_on_error_openai_exception(monkeypatch):
    monkeypatch.setenv("LOCAL_FALLBACK_POLICY", "on_error")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class BoomClient(DummyClient):
        class responses:
            @staticmethod
            def create(*args, **kwargs):
                raise RuntimeError("boom")

    monkeypatch.setattr("app.llm.openai_client.build_client", lambda: BoomClient())
    monkeypatch.setattr("app.services.llm.build_client", lambda: BoomClient())
    res = process_single_file("note.txt", b"hi")
    meta = res["_meta"]
    assert meta["llm_used"] == "local"
    assert meta["fallback_reason"].startswith("openai_error")
    monkeypatch.delenv("LOCAL_FALLBACK_POLICY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)


def test_fallback_policy_never_missing_key(monkeypatch):
    monkeypatch.setenv("LOCAL_FALLBACK_POLICY", "never")
    try:
        process_single_file("note.txt", b"hi")
    except OpenAIConfigError:
        pass
    else:
        assert False, "Expected OpenAIConfigError"
    monkeypatch.delenv("LOCAL_FALLBACK_POLICY", raising=False)


def test_fallback_policy_if_no_key(monkeypatch):
    monkeypatch.setenv("LOCAL_FALLBACK_POLICY", "if_no_key")
    res = process_single_file("note.txt", b"hi")
    meta = res["_meta"]
    assert meta["llm_used"] == "local"
    assert meta["fallback_reason"] == "no_api_key"
    monkeypatch.delenv("LOCAL_FALLBACK_POLICY", raising=False)


def test_fallback_policy_if_no_key_openai_error(monkeypatch):
    monkeypatch.setenv("LOCAL_FALLBACK_POLICY", "if_no_key")
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
    monkeypatch.delenv("LOCAL_FALLBACK_POLICY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
