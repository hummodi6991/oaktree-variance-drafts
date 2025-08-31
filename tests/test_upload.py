import os
from pathlib import Path

os.environ["API_KEY"] = "testkey"
os.environ["REQUIRE_API_KEY"] = "true"

from fastapi.testclient import TestClient
from app.main import app


def test_upload_endpoint():
    client = TestClient(app)
    base = Path("data/templates")
    with (
        (base / "budget_actuals.csv").open("rb") as ba,
        (base / "change_orders.csv").open("rb") as co,
        (base / "vendor_map.csv").open("rb") as vm,
        (base / "category_map.csv").open("rb") as cm,
    ):
        files = {
            "budget_actuals": ("budget_actuals.csv", ba, "text/csv"),
            "change_orders": ("change_orders.csv", co, "text/csv"),
            "vendor_map": ("vendor_map.csv", vm, "text/csv"),
            "category_map": ("category_map.csv", cm, "text/csv"),
        }
        data = {
            "materiality_pct": "5",
            "materiality_amount_sar": "100000",
            "bilingual": "true",
            "enforce_no_speculation": "true",
            "api_key": "testkey",
        }
        resp = client.post("/upload", files=files, data=data)
    assert resp.status_code == 200
    result = resp.json()
    assert isinstance(result, list)
    assert all("draft_en" in item for item in result)
