from pathlib import Path
from fastapi.testclient import TestClient

from app.main import app
import app.api as _api  # noqa: F401 - ensure /single/generate route is registered


def test_single_generate_returns_procurement_cards():
    client = TestClient(app)
    pdf_path = Path(__file__).resolve().parent.parent / "samples" / "procurement_example.pdf"
    with pdf_path.open("rb") as f:
        resp = client.post("/single/generate", files={"file": ("sample.pdf", f, "application/pdf")})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("mode") == "procurement"
    assert isinstance(data.get("cards"), list)
    assert len(data["cards"]) > 0
