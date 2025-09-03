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


def test_upload_single_data_file():
    client = TestClient(app)
    files = {
        "data_file": ("notes.txt", b"item a 100\nitem b 200", "text/plain"),
    }
    resp = client.post("/upload", files=files, data={"api_key": "testkey"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    cards = data["procurement_summary"]
    assert any(c.get("description") for c in cards)
    assert all("item_code" in c for c in cards)


def test_upload_mutual_exclusive():
    client = TestClient(app)
    base = Path("data/templates")
    with (base / "budget_actuals.csv").open("rb") as ba:
        files = {
            "budget_actuals": ("budget_actuals.csv", ba, "text/csv"),
            "data_file": ("notes.txt", b"item 1", "text/plain"),
        }
        resp = client.post("/upload", files=files, data={"api_key": "testkey"})
    assert resp.status_code == 400


def test_pdf_fallback(monkeypatch):
    client = TestClient(app)
    monkeypatch.setattr("app.main.pdf_extract_text", lambda *a, **k: "")

    class DummyPage:
        def extract_text(self):
            return "item a 100\nitem b 200"

    class DummyPDF:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

        @property
        def pages(self):
            return [DummyPage()]

    import types

    monkeypatch.setattr("app.main.pdfplumber", types.SimpleNamespace(open=lambda *a, **k: DummyPDF()))

    files = {"data_file": ("test.pdf", b"%PDF-1.4", "application/pdf")}
    resp = client.post("/upload", files=files, data={"api_key": "testkey"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    assert data["total_amount_sar"] == 300
    assert len(data["procurement_summary"]) == 2
    assert all("item_code" in c for c in data["procurement_summary"])


def test_upload_llm_failure(monkeypatch):
    """Ensure missing LLM does not crash single-file upload."""
    client = TestClient(app)
    monkeypatch.setattr("app.main.extract_items_via_llm", lambda *_: (_ for _ in ()).throw(RuntimeError("no model")))
    files = {"data_file": ("empty.pdf", b"", "application/pdf")}
    resp = client.post("/upload", files=files, data={"api_key": "testkey"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0
    assert data["procurement_summary"] == []


def test_extract_freeform_procurement_summary():
    client = TestClient(app)
    file_content = b"co_id\nD01\nD02\n"
    files = {"files": ("test.csv", file_content, "text/csv")}
    resp = client.post("/extract/freeform", files=files)
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    cards = data["procurement_summary"]
    assert len(cards) == 2
    assert all(c["evidence_link"] == "Uploaded procurement file" for c in cards)
    assert all("draft_en" in c and "draft_ar" in c for c in cards)
    assert all("item_code" in c for c in cards)
