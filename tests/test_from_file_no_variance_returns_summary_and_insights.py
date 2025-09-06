from fastapi.testclient import TestClient
from app.main import app, require_api_key

app.dependency_overrides[require_api_key] = lambda: None

def test_from_file_no_variance():
    client = TestClient(app)
    files = {"file": ("note.txt", b"Procurement lines only, no budget/actuals", "text/plain")}
    r = client.post("/drafts/from-file", files=files)
    assert r.status_code == 200
    j = r.json()
    assert j["kind"] == "insights"
    assert "summary_text" in j
    assert "analysis_text" in j and isinstance(j["analysis_text"], str)
    assert "insights_text" in j and isinstance(j["insights_text"], str)
    assert "analysis" not in j and "insights" not in j
