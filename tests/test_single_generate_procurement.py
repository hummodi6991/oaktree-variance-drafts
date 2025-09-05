from pathlib import Path
from fastapi.testclient import TestClient

from app.main import app
import app.api as _api  # noqa: F401 - ensure /single/generate route is registered


def test_single_generate_returns_summary_analysis_insights():
    client = TestClient(app)
    pdf_path = Path(__file__).resolve().parent.parent / "samples" / "procurement_example.pdf"
    with pdf_path.open("rb") as f:
        resp = client.post("/single/generate", files={"file": ("sample.pdf", f, "application/pdf")})
    assert resp.status_code == 200
    data = resp.json()
    assert "summary_text" in data
    assert "analysis" in data and isinstance(data["analysis"], dict)
    assert "insights" in data and isinstance(data["insights"], dict)
