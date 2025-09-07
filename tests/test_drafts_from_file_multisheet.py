import io
import pandas as pd
from fastapi.testclient import TestClient

from app.main import app, require_api_key

app.dependency_overrides[require_api_key] = lambda: None


def _multi_sheet_bytes() -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        pd.DataFrame({"foo": [1]}).to_excel(xl, index=False, sheet_name="Sheet1")
        pd.DataFrame(
            {
                "project_id": ["P1"],
                "period": ["2024-01"],
                "cost_code": ["C1"],
                "budget_sar": [100],
                "actual_sar": [120],
            }
        ).to_excel(xl, index=False, sheet_name="Budget")
    return buf.getvalue()


def test_drafts_from_file_returns_insights():
    client = TestClient(app)
    files = {
        "file": (
            "multi.xlsx",
            _multi_sheet_bytes(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    }
    resp = client.post("/drafts/from-file", files=files)
    assert resp.status_code == 200
    data = resp.json()
    assert data["kind"] == "insights"
    assert "summary_text" in data
    assert "analysis_text" in data and isinstance(data["analysis_text"], str)
    assert "insights_text" in data and isinstance(data["insights_text"], str)
    assert "analysis" not in data and "insights" not in data
