import os
from pathlib import Path
from io import BytesIO
import pandas as pd

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


def test_upload_endpoint_excel():
    client = TestClient(app)
    base = Path("data/templates")

    def to_xlsx(name: str) -> BytesIO:
        df = pd.read_csv(base / name)
        buf = BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        return buf

    files = {
        "budget_actuals": (
            "budget_actuals.xlsx",
            to_xlsx("budget_actuals.csv"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "change_orders": (
            "change_orders.xlsx",
            to_xlsx("change_orders.csv"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "vendor_map": (
            "vendor_map.xlsx",
            to_xlsx("vendor_map.csv"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "category_map": (
            "category_map.xlsx",
            to_xlsx("category_map.csv"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
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
