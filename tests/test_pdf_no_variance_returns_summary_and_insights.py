from fastapi.testclient import TestClient
from app.main import app, require_api_key
import pathlib

app.dependency_overrides[require_api_key] = lambda: None

def test_pdf_no_variance():
    client = TestClient(app)
    pdf_path = pathlib.Path('samples/procurement_example.pdf')
    files = {"file": (pdf_path.name, pdf_path.read_bytes(), "application/pdf")}
    r = client.post("/drafts/from-file", files=files)
    assert r.status_code == 200
    j = r.json()
    assert j["kind"] == "insights"
    assert "summary" in j and "analysis" in j and "insights" in j
    assert "items" not in j["summary"]
    assert "message" in j and "budget-vs-actual" in j["message"].lower()
