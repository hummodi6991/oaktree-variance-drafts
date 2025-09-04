import io
import pandas as pd
from fastapi.testclient import TestClient

from app.main import app


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


def test_drafts_from_file_multisheet_variance():
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
    assert "variance_items" in data
    assert len(data["variance_items"]) == 1
    v = data["variance_items"][0]
    assert v["budget_sar"] == 100.0
    assert v["actual_sar"] == 120.0
    assert v["variance_sar"] == 20.0
