
import os
import io
import uuid
import csv  # noqa: F401
import json  # noqa: F401
import statistics
from typing import Any, Dict, List, Optional
import re
import textwrap

import chardet
import pandas as pd
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except Exception:  # pragma: no cover - optional dependency
    pdf_extract_text = None
try:
    import pdfplumber
except Exception:  # pragma: no cover - optional dependency
    pdfplumber = None  # type: ignore
try:
    import docx
except Exception:  # pragma: no cover - optional dependency
    docx = None
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    Header,
    HTTPException,
    UploadFile,
    Form,
)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .schemas import DraftRequest, DraftResponse, ProcurementItem, VendorSnapshot
from .pipeline import generate_drafts
from .services.csv_loader import parse_tabular
from app.services.singlefile import process_single_file, draft_bilingual_procurement_card
from app.parsers.single_file_intake import parse_single_file
from .llm.extract_from_text import extract_items_via_llm
from app.parsers.single_file import analyze_single_file

app: FastAPI = FastAPI(title="Oaktree Variance Drafts API", version="0.1.0")

SAFE_CO_COLS = [
    "co_id",
    "date",
    "amount_sar",
    "description",
    "linked_cost_code",
    "project_id",
    "vendor_name",
    "file_link",
    "quantity",
    "unit_price",
    "currency",
    "vat_rate",
    "inclusions",
    "exclusions",
    "notes",
    # Budget/actual fields (for free-form uploads)
    "budget_sar",
    "actual_sar",
    "period",
    "cost_code",
    "category",
]


def _df_from_bytes(name: str, data: bytes) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".csv"):
        enc = chardet.detect(data).get("encoding") or "utf-8"
        return pd.read_csv(io.BytesIO(data), encoding=enc)
    if low.endswith(".xlsx") or low.endswith(".xls"):
        return pd.read_excel(io.BytesIO(data))
    try:
        enc = chardet.detect(data).get("encoding") or "utf-8"
        return pd.read_csv(io.BytesIO(data), encoding=enc)
    except Exception:
        return pd.DataFrame()


def _text_from_bytes(name: str, data: bytes) -> str:
    low = name.lower()
    if low.endswith(".pdf"):
        text = ""
        if pdf_extract_text:
            try:
                text = pdf_extract_text(io.BytesIO(data)) or ""
            except Exception:
                text = ""
        if not text.strip() and pdfplumber:
            try:
                with pdfplumber.open(io.BytesIO(data)) as pdf:
                    text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            except Exception:
                text = ""
        return text
    if low.endswith(".docx") and docx:
        try:
            d = docx.Document(io.BytesIO(data))
            return "\n".join(p.text for p in d.paragraphs)
        except Exception:
            return ""
    enc = chardet.detect(data).get("encoding") or "utf-8"
    try:
        return data.decode(enc, errors="ignore")
    except Exception:
        return ""


def _rows_from_tablelike(df: pd.DataFrame) -> List[Dict[str, Any]]:
    colmap: Dict[str, str] = {}
    for c in df.columns:
        k = c.strip().lower()
        if k in ["co id", "co_id", "#", "id"]:
            colmap["co_id"] = c
        elif k in ["date", "co_date", "doc_date"]:
            colmap["date"] = c
        elif k in ["amount", "amount_sar", "value", "sar", "total_sar"]:
            colmap["amount_sar"] = c
        elif k in ["desc", "description", "scope", "item", "line_item"]:
            colmap["description"] = c
        elif k in ["cost_code", "linked_cost_code", "costcode", "code"]:
            colmap["linked_cost_code"] = c
        elif k in ["project", "project_id", "project name", "projectname"]:
            colmap["project_id"] = c
        elif k in ["vendor", "vendor_name", "supplier", "supplier_name"]:
            colmap["vendor_name"] = c
        elif k in ["file", "file_link", "evidence", "link", "url"]:
            colmap["file_link"] = c
        elif k in ["qty", "quantity", "qty."]:
            colmap["quantity"] = c
        elif k in ["unit_price", "unit price", "price", "unit_cost", "rate", "unit cost"]:
            colmap["unit_price"] = c
        elif k in ["currency", "curr", "cur"]:
            colmap["currency"] = c
        elif k in ["vat", "vat_rate", "vat%", "vat %", "vat rate"]:
            colmap["vat_rate"] = c
        elif k in ["inclusions", "includes", "inclusion"]:
            colmap["inclusions"] = c
        elif k in ["exclusions", "excludes", "exclusion"]:
            colmap["exclusions"] = c
        elif k in ["notes", "remarks", "comment", "comments"]:
            colmap["notes"] = c
    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        item: Dict[str, Any] = {}
        for k in SAFE_CO_COLS:
            if k in colmap:
                v = r[colmap[k]]
                if pd.isna(v):
                    v = None
                item[k] = v
            else:
                item[k] = None
        if item.get("amount_sar") is not None:
            try:
                item["amount_sar"] = float(str(item["amount_sar"]).replace(",", ""))
            except Exception:
                item["amount_sar"] = None
        if item.get("quantity") is not None:
            try:
                item["quantity"] = float(str(item["quantity"]).replace(",", ""))
            except Exception:
                item["quantity"] = None
        if item.get("unit_price") is not None:
            try:
                item["unit_price"] = float(str(item["unit_price"]).replace(",", ""))
            except Exception:
                item["unit_price"] = None
        if item.get("vat_rate") is not None:
            try:
                item["vat_rate"] = float(str(item["vat_rate"]).replace("%", "").replace(",", ""))
            except Exception:
                item["vat_rate"] = None
        if item.get("amount_sar") is None and item.get("quantity") is not None and item.get("unit_price") is not None:
            item["amount_sar"] = item["quantity"] * item["unit_price"]
        out.append(item)
    return out


def _rows_from_budget_actuals(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Tolerant parser for budget vs actual style tables.

    Looks for project, period, cost code/category along with budget and
    actual columns. Missing columns are filled with ``None`` so downstream
    logic can still operate on partially-complete rows.
    """
    colmap: Dict[str, str] = {}
    for c in df.columns:
        k = c.strip().lower()
        if k in ["project", "project_id", "project name", "projectname"]:
            colmap["project_id"] = c
        elif k in ["period", "month", "yyyymm", "yyyy-mm", "period(yyyy-mm)"]:
            colmap["period"] = c
        elif k in ["cost_code", "costcode", "code", "linked_cost_code"]:
            colmap["cost_code"] = c
        elif k in ["category", "cost_category", "type"]:
            colmap["category"] = c
        elif k in [
            "budget",
            "budget_sar",
            "budget amount",
            "budget_amt",
            "planned",
            "planned_budget",
        ]:
            colmap["budget_sar"] = c
        elif k in [
            "actual",
            "actual_sar",
            "actual amount",
            "actual_amt",
            "spent",
        ]:
            colmap["actual_sar"] = c

    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        item: Dict[str, Any] = {}
        for k in [
            "project_id",
            "period",
            "cost_code",
            "category",
            "budget_sar",
            "actual_sar",
        ]:
            v = r[colmap[k]] if k in colmap else None
            if pd.isna(v):
                v = None
            if k in ("budget_sar", "actual_sar") and v is not None:
                try:
                    v = float(str(v).replace(",", ""))
                except Exception:
                    v = None
            item[k] = v
        # Ignore rows lacking both budget and actual figures
        if item.get("budget_sar") is None and item.get("actual_sar") is None:
            continue
        out.append(item)
    return out


def _rows_from_budget_actuals_text(text: str) -> List[Dict[str, Any]]:
    """Parse free-form text lines for budget vs actual pairs."""
    rows: List[Dict[str, Any]] = []
    if not text.strip():
        return rows
    budget_re = re.compile(r"(budget|planned)[^0-9]*([0-9][0-9,\.]+)", re.I)
    actual_re = re.compile(r"(actual|spent)[^0-9]*([0-9][0-9,\.]+)", re.I)
    for line in text.splitlines():
        b_match = budget_re.search(line)
        a_match = actual_re.search(line)
        if not (b_match and a_match):
            continue
        try:
            budget = float(b_match.group(2).replace(",", ""))
            actual = float(a_match.group(2).replace(",", ""))
        except Exception:
            continue
        rows.append(
            {
                "project_id": None,
                "period": None,
                "cost_code": None,
                "category": None,
                "budget_sar": budget,
                "actual_sar": actual,
            }
        )
    return rows


def _rows_from_text(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not text.strip():
        return rows
    for line in text.splitlines():
        if not line.strip():
            continue
        maybe_amount = None
        for token in line.replace(",", "").split():
            try:
                val = float(token)
                if val > 0:
                    maybe_amount = val
                    break
            except Exception:
                pass
        item = {k: None for k in SAFE_CO_COLS}
        item["description"] = line.strip()[:280]
        item["amount_sar"] = maybe_amount
        rows.append(item)
    return rows


def _extract_rows_via_llm(text: str) -> List[Dict[str, Any]]:
    """Fallback extraction via LLM; swallow errors if model unavailable."""
    try:
        return extract_items_via_llm(text)
    except Exception:
        return []


def _build_procurement_summary(rows: List[Dict[str, Any]], bilingual: bool = True) -> List[ProcurementItem]:
    """Convert raw row dicts to ProcurementItem cards."""
    out: List[ProcurementItem] = []
    for r in rows:
        # Skip rows that are entirely empty but keep partially-filled ones
        if all(r.get(k) in (None, "") for k in SAFE_CO_COLS):
            continue

        parts_en: List[str] = []
        if r.get("co_id"):
            parts_en.append(f"Item {r['co_id']}")
        if r.get("description"):
            parts_en.append(str(r["description"]))
        if r.get("vendor_name"):
            parts_en.append(f"from {r['vendor_name']}")
        if r.get("amount_sar") is not None:
            parts_en.append(f"for SAR {r['amount_sar']:,.0f}")
        en = " ".join(parts_en).strip()
        ar = None
        if bilingual:
            parts_ar: List[str] = []
            if r.get("co_id"):
                parts_ar.append(f"البند {r['co_id']}")
            if r.get("description"):
                parts_ar.append(str(r["description"]))
            if r.get("vendor_name"):
                parts_ar.append(f"من {r['vendor_name']}")
            if r.get("amount_sar") is not None:
                parts_ar.append(f"بقيمة {r['amount_sar']:,.0f} ريال")
            ar = " ".join(parts_ar).strip()
        out.append(
            ProcurementItem(
                item_code=r.get("co_id") or r.get("linked_cost_code"),
                description=r.get("description"),
                quantity=r.get("quantity"),
                unit_price=r.get("unit_price"),
                amount_sar=r.get("amount_sar"),
                vendor=r.get("vendor_name"),
                document_date=r.get("date"),
                evidence_link=r.get("file_link") or "Uploaded procurement file",
                draft_en=en,
                draft_ar=ar,
            )
        )
    return out


def _build_vendor_snapshots(rows: List[Dict[str, Any]]) -> List[VendorSnapshot]:
    out: List[VendorSnapshot] = []
    by_vendor: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        vendor = r.get("vendor_name")
        if not vendor:
            continue
        by_vendor.setdefault(vendor, []).append(r)
    for vendor, items in by_vendor.items():
        total = sum(float(it.get("amount_sar") or 0) for it in items)
        vat_rate = next((it.get("vat_rate") for it in items if it.get("vat_rate") is not None), None)
        currency = next((it.get("currency") for it in items if it.get("currency") is not None), None)
        quote_date = next((it.get("date") for it in items if it.get("date") is not None), None)
        total_incl = total * (1 + (vat_rate or 0) / 100) if total else None
        out.append(
            VendorSnapshot(
                vendor=vendor,
                quote_date=quote_date,
                currency=currency,
                vat_rate=vat_rate,
                total_excl_vat=total or None,
                total_incl_vat=total_incl,
            )
        )
    return out


def _build_bid_comparison(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grid: Dict[str, Dict[str, float]] = {}
    for r in rows:
        item = r.get("co_id") or r.get("linked_cost_code") or r.get("description")
        vendor = r.get("vendor_name") or "Unknown"
        amt = r.get("amount_sar")
        if item is None or amt is None:
            continue
        grid.setdefault(str(item), {})[vendor] = float(amt)
    out: List[Dict[str, Any]] = []
    for item, vendor_prices in grid.items():
        if not vendor_prices:
            continue
        min_vendor = min(vendor_prices, key=vendor_prices.get)
        amounts = list(vendor_prices.values())
        med = statistics.median(amounts) if amounts else None
        row: Dict[str, Any] = {"item_code": item}
        for vendor, amount in vendor_prices.items():
            variance_vs_median = amount - med if med is not None else None
            pct_spread = (
                (variance_vs_median / med) * 100 if (med and variance_vs_median is not None) else None
            )
            row[vendor] = {
                "amount_sar": amount,
                "is_lowest": vendor == min_vendor,
                "variance_vs_median": variance_vs_median,
                "pct_spread_vs_median": pct_spread,
            }
        out.append(row)
    return out


def _compute_best_mix(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    prices: Dict[str, Dict[str, float]] = {}
    vendor_totals: Dict[str, float] = {}
    for r in rows:
        item = r.get("co_id") or r.get("linked_cost_code") or r.get("description")
        vendor = r.get("vendor_name") or "Unknown"
        amt = r.get("amount_sar")
        if item is None or amt is None:
            continue
        prices.setdefault(str(item), {})[vendor] = float(amt)
        vendor_totals[vendor] = vendor_totals.get(vendor, 0.0) + float(amt)
    best_total = 0.0
    for vp in prices.values():
        best_total += min(vp.values())
    cheapest_vendor = None
    single_total = None
    if vendor_totals:
        cheapest_vendor = min(vendor_totals, key=vendor_totals.get)
        single_total = vendor_totals[cheapest_vendor]
    savings = None
    if single_total is not None:
        savings = single_total - best_total
    return {
        "best_mix_total_sar": best_total if best_total else None,
        "cheapest_single_vendor": cheapest_vendor,
        "single_vendor_total_sar": single_total,
        "estimated_savings_sar": savings,
    }


@app.post("/extract/freeform")
async def extract_freeform(files: List[UploadFile] = File(...)) -> Dict[str, Any]:
    """Accept CSV/XLSX/DOCX/PDF/TXT and return tolerant row dicts.

    The return payload includes a ``mode`` field indicating whether the rows
    resemble change-order data (``change_orders``) or budget-vs-actual data
    (``budget_actuals``). This allows the caller to route the rows to the
    appropriate pipeline.
    """
    all_rows: List[Dict[str, Any]] = []
    mode = "change_orders"
    for f in files:
        data = await f.read()
        name = f.filename or "upload.bin"
        low = name.lower()
        rows: List[Dict[str, Any]] = []
        if any(low.endswith(ext) for ext in [".csv", ".xlsx", ".xls"]):
            df = _df_from_bytes(name, data)
            if not df.empty:
                co_rows = _rows_from_tablelike(df)
                ba_rows = _rows_from_budget_actuals(df)
                if ba_rows and not any(r.get("amount_sar") for r in co_rows):
                    rows = ba_rows
                    mode = "budget_actuals"
                else:
                    rows = co_rows
        elif any(low.endswith(ext) for ext in [".pdf", ".docx", ".txt", ".md", ".rtf"]):
            text = _text_from_bytes(name, data)
            rows = _rows_from_text(text)
            if not rows:
                for it in _extract_rows_via_llm(text):
                    rows.append({
                        "project_id": None,
                        "linked_cost_code": None,
                        "description": it.get("description"),
                        "file_link": None,
                        "co_id": it.get("co_id"),
                        "date": None,
                        "amount_sar": it.get("amount_sar"),
                        "vendor_name": None,
                    })
        else:
            df = _df_from_bytes(name, data)
            if not df.empty:
                co_rows = _rows_from_tablelike(df)
                ba_rows = _rows_from_budget_actuals(df)
                if ba_rows and not any(r.get("amount_sar") for r in co_rows):
                    rows = ba_rows
                    mode = "budget_actuals"
                else:
                    rows = co_rows
            else:
                text = _text_from_bytes(name, data)
                rows = _rows_from_text(text)
                if not rows:
                    for it in _extract_rows_via_llm(text):
                        rows.append({
                            "project_id": None,
                            "linked_cost_code": None,
                            "description": it.get("description"),
                            "file_link": None,
                            "co_id": it.get("co_id"),
                            "date": None,
                            "amount_sar": it.get("amount_sar"),
                            "vendor_name": None,
                        })
        all_rows.extend(rows)
    filtered = [
        r
        for r in all_rows
        if any(v is not None and str(v).strip() != "" for v in r.values())
    ]
    if mode == "budget_actuals":
        cards: List[ProcurementItem] = []
        count = len(filtered)
    else:
        cards = _build_procurement_summary(filtered)
        count = len(cards)
    return {
        "rows": filtered,
        "procurement_summary": [c.model_dump() for c in cards],
        "count": count,
        "mode": mode,
    }

# Static UI for CEO
try:
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
except Exception:
    # Already mounted or no static dir; ignore
    pass

REQUIRE_API_KEY = os.getenv("REQUIRE_API_KEY", "true").lower() == "true"
API_KEY = os.getenv("API_KEY", "")


def require_api_key(x_api_key: str | None = Header(default=None, alias="x-api-key")) -> None:
    if not REQUIRE_API_KEY:
        return
    if not API_KEY or not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


deps = [Depends(require_api_key)] if REQUIRE_API_KEY else []

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/diag/openai")
def diag_openai():
    """Quick connectivity check to the model provider."""
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    try:
        import time
        from openai import OpenAI

        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        timeout = int(os.getenv("OPENAI_TIMEOUT", "10"))
        client = OpenAI(api_key=api_key, timeout=timeout, max_retries=0)
        t0 = time.time()
        resp = client.responses.create(model=model, input="ping")
        ms = int((time.time() - t0) * 1000)
        return {"ok": True, "model": model, "latency_ms": ms, "id": getattr(resp, "id", None)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "model": model, "error": str(e)})


# -------- Job tracking --------
class JobStatus(BaseModel):
    job_id: str
    status: str          # queued | running | done | error
    progress: int        # 0..100
    message: Optional[str] = None
    result: Optional[dict] = None


jobs: Dict[str, Dict[str, Any]] = {}


def _set_job(job_id: str, **kw):
    if job_id in jobs:
        jobs[job_id].update(kw)


@app.post("/drafts/async", response_model=JobStatus, dependencies=deps)
async def create_drafts_async(
    req: DraftRequest, background_tasks: BackgroundTasks
):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "result": None,
    }

    def run_job():
        try:
            _set_job(job_id, status="running", progress=5, message="Validating input")

            def cb(pct: int, msg: str = ""):
                _set_job(
                    job_id,
                    progress=max(0, min(100, int(pct))),
                    message=msg or jobs[job_id].get("message"),
                )

            result = generate_drafts(req, progress_cb=cb)
            _set_job(
                job_id,
                status="done",
                progress=100,
                message="Completed",
                result=result,
            )
        except Exception as e:  # pragma: no cover - error path
            _set_job(job_id, status="error", message=str(e))

    background_tasks.add_task(run_job)
    return JobStatus(**jobs[job_id])


@app.get("/jobs/{job_id}", response_model=JobStatus, dependencies=deps)
async def get_job(job_id: str):
    data = jobs.get(job_id)
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatus(**data)


@app.get("/ui", include_in_schema=False)
def ceo_ui():
    return FileResponse("app/templates/ui.html")

@app.post("/drafts", response_model=List[DraftResponse], dependencies=deps)
def create_drafts(req: DraftRequest):
    """Create EN/AR variance explanation drafts."""
    return generate_drafts(req)


@app.post("/upload", include_in_schema=False)
async def upload(
    budget_actuals: UploadFile | None = File(None),
    change_orders: UploadFile | None = File(None),
    vendor_map: UploadFile | None = File(None),
    category_map: UploadFile | None = File(None),
    data_file: UploadFile | None = File(None),
    materiality_pct: int = Form(5),
    materiality_amount_sar: int = Form(100000),
    bilingual: bool = Form(True),
    enforce_no_speculation: bool = Form(True),
    api_key: Optional[str] = Form(None),
):
    # Optional simple API key check using the same header logic
    from app.schemas import (
        BudgetActualRow,
        ChangeOrderRow,
        VendorMapRow,
        CategoryMapRow,
        ConfigModel,
    )

    if REQUIRE_API_KEY and (not API_KEY or not api_key or api_key != API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    # Track B: single freeform file
    if data_file is not None:
        if any(f is not None for f in [budget_actuals, change_orders, vendor_map, category_map]):
            raise HTTPException(status_code=400, detail="Provide either the 4 structured files or a single data_file")
        data = await data_file.read()
        name = data_file.filename or "upload"
        rows: List[Dict[str, Any]] = []
        mode = "change_orders"
        text = ""
        df = _df_from_bytes(name, data)
        if not df.empty:
            co_rows = _rows_from_tablelike(df)
            ba_rows = _rows_from_budget_actuals(df)
            if ba_rows and not any(r.get("amount_sar") for r in co_rows):
                rows = ba_rows
                mode = "budget_actuals"
            else:
                rows = co_rows
            # If every field in every row is blank, treat as empty and fallback to text parsing
            if all(all(v in (None, "") for v in r.values()) for r in rows):
                rows = []
        if not rows:
            text = _text_from_bytes(name, data)
            ba_text = _rows_from_budget_actuals_text(text)
            if ba_text:
                rows = ba_text
                mode = "budget_actuals"
            else:
                rows = _rows_from_text(text)
                if not rows:
                    for it in _extract_rows_via_llm(text):
                        rows.append({
                            "project_id": None,
                            "linked_cost_code": None,
                            "description": it.get("description"),
                            "file_link": None,
                            "co_id": it.get("co_id"),
                            "date": None,
                            "amount_sar": it.get("amount_sar"),
                            "vendor_name": None,
                        })
        filtered = [
            r for r in rows if any(v is not None and str(v).strip() != "" for v in r.values())
        ]
        if mode != "budget_actuals" and (
            not filtered or not any(r.get("amount_sar") for r in filtered)
        ):
            if (text or "").strip():
                summary = textwrap.shorten((text or "").strip(), width=200, placeholder="...")
                return {"mode": "summary", "summary": summary}
        if mode == "budget_actuals":
            if not filtered:
                return {
                    "mode": mode,
                    "drafts": [],
                    "paired_count": 0,
                    "unpaired_count": 0,
                    "unpaired_rows": [],
                    "unpaired_summary": {
                        "total_budget_sar": 0.0,
                        "total_actual_sar": 0.0,
                    },
                }

            paired = [
                r
                for r in filtered
                if r.get("budget_sar") is not None and r.get("actual_sar") is not None
            ]
            unpaired = [r for r in filtered if r not in paired]

            drafts = []
            if paired:
                ba_models = [
                    BudgetActualRow(
                        project_id=r.get("project_id") or "Unknown",
                        period=r.get("period") or "1970-01",
                        cost_code=r.get("cost_code")
                        or r.get("linked_cost_code")
                        or "UNKNOWN",
                        category=r.get("category"),
                        budget_sar=float(r.get("budget_sar") or 0),
                        actual_sar=float(r.get("actual_sar") or 0),
                        currency=r.get("currency"),
                        remarks=r.get("remarks"),
                    )
                    for r in paired
                ]
                cfg = ConfigModel(
                    materiality_pct=materiality_pct,
                    materiality_amount_sar=materiality_amount_sar,
                    bilingual=bilingual,
                    enforce_no_speculation=enforce_no_speculation,
                )
                req = DraftRequest(
                    budget_actuals=ba_models,
                    change_orders=[],
                    vendor_map=[],
                    category_map=[],
                    config=cfg,
                )
                drafts = generate_drafts(req)

            unpaired_summary = {
                "total_budget_sar": sum(
                    float(r.get("budget_sar") or 0) for r in unpaired
                ),
                "total_actual_sar": sum(
                    float(r.get("actual_sar") or 0) for r in unpaired
                ),
            }
            return {
                "mode": mode,
                "drafts": [d.model_dump() for d in drafts],
                "paired_count": len(paired),
                "unpaired_count": len(unpaired),
                "unpaired_rows": unpaired,
                "unpaired_summary": unpaired_summary,
            }
        # change order style report
        cards = _build_procurement_summary(filtered, bilingual=bilingual)
        snapshots = _build_vendor_snapshots(filtered)
        bid_grid = _build_bid_comparison(filtered)
        best_mix = _compute_best_mix(filtered)
        total = sum(c.amount_sar or 0 for c in cards)
        item_table = [
            {
                "item_code": r.get("co_id") or r.get("linked_cost_code"),
                "description": r.get("description"),
                "quantity": r.get("quantity"),
                "unit_price": r.get("unit_price"),
                "line_total_sar": r.get("amount_sar"),
                "inclusions": r.get("inclusions"),
                "exclusions": r.get("exclusions"),
                "notes": r.get("notes"),
                "vendor": r.get("vendor_name"),
            }
            for r in filtered
        ]
        exclusions_audit = sorted({r["exclusions"] for r in filtered if r.get("exclusions")})
        readiness = "green" if snapshots else "red"
        return {
            "procurement_summary": [c.model_dump() for c in cards],
            "vendor_snapshots": [s.model_dump() for s in snapshots],
            "item_table": item_table,
            "bid_comparison": bid_grid,
            "best_mix": best_mix,
            "exclusions_audit": exclusions_audit,
            "readiness_to_po": readiness,
            "count": len(cards),
            "total_amount_sar": total,
        }

    # Track A: four structured files
    if not all([budget_actuals, change_orders, vendor_map, category_map]):
        raise HTTPException(status_code=400, detail="Missing one or more required files")

    ba = await budget_actuals.read()
    co = await change_orders.read()
    vm = await vendor_map.read()
    cm = await category_map.read()
    df_ba = _read_tabular(ba, budget_actuals.filename).fillna("")
    df_co = _read_tabular(co, change_orders.filename).fillna("")
    df_vm = _read_tabular(vm, vendor_map.filename).fillna("")
    df_cm = _read_tabular(cm, category_map.filename).fillna("")

    ba_rows = [
        BudgetActualRow(**(r._asdict() if hasattr(r, "_asdict") else dict(r)))
        for r in df_ba.to_dict(orient="records")
    ]
    co_rows = [ChangeOrderRow(**dict(r)) for r in df_co.to_dict(orient="records")]
    vm_rows = [VendorMapRow(**dict(r)) for r in df_vm.to_dict(orient="records")]
    cm_rows = [CategoryMapRow(**dict(r)) for r in df_cm.to_dict(orient="records")]

    cfg = ConfigModel(
        materiality_pct=materiality_pct,
        materiality_amount_sar=materiality_amount_sar,
        bilingual=bilingual,
        enforce_no_speculation=enforce_no_speculation,
    )

    req = DraftRequest(
        budget_actuals=ba_rows,
        change_orders=co_rows,
        vendor_map=vm_rows,
        category_map=cm_rows,
        config=cfg,
    )

    result = generate_drafts(req)
    return result


@app.post("/ui/parse-csv")
async def parse_csv_to_request(
    budget_actuals: UploadFile = File(...),
    change_orders: UploadFile = File(...),
    vendor_map: UploadFile = File(...),
    category_map: UploadFile = File(...),
    materiality_pct: int = Form(5),
    materiality_amount_sar: int = Form(100000),
    bilingual: bool = Form(True),
    enforce_no_speculation: bool = Form(True),
):
    """Parse CSV/XLS/XLSX uploads into a JSON ``DraftRequest`` payload."""
    payload = {
        "budget_actuals": parse_tabular(
            await budget_actuals.read(), budget_actuals.filename or "budget_actuals"
        ),
        "change_orders": parse_tabular(
            await change_orders.read(), change_orders.filename or "change_orders"
        ),
        "vendor_map": parse_tabular(
            await vendor_map.read(), vendor_map.filename or "vendor_map"
        ),
        "category_map": parse_tabular(
            await category_map.read(), category_map.filename or "category_map"
        ),
        "config": {
            "materiality_pct": int(materiality_pct),
            "materiality_amount_sar": int(materiality_amount_sar),
            "bilingual": bool(bilingual)
            if isinstance(bilingual, bool)
            else str(bilingual).lower() == "true",
            "enforce_no_speculation": bool(enforce_no_speculation)
            if isinstance(enforce_no_speculation, bool)
            else str(enforce_no_speculation).lower() == "true",
        },
    }
    return JSONResponse(payload)


def _read_tabular(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Read CSV/XLS/XLSX to DataFrame with UTF-8 fallback handling."""
    name = (filename or "").lower()
    if name.endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(file_bytes))
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(file_bytes), encoding="latin-1")
    if name.endswith(".xls") or name.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(file_bytes))
    raise HTTPException(
        status_code=400,
        detail=f"Unsupported file type for {filename}. Use .csv, .xls, or .xlsx",
    )


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    df = df.rename(columns={c: c.strip() for c in df.columns})
    return df.to_dict(orient="records")


def _build_payload(
    budget_actuals_rows: List[Dict[str, Any]],
    change_orders_rows: List[Dict[str, Any]],
    vendor_map_rows: List[Dict[str, Any]],
    category_map_rows: List[Dict[str, Any]],
    materiality_pct: float,
    materiality_amount_sar: float,
    bilingual: bool,
    enforce_no_speculation: bool,
) -> Dict[str, Any]:
    return {
        "budget_actuals": budget_actuals_rows,
        "change_orders": change_orders_rows,
        "vendor_map": vendor_map_rows,
        "category_map": category_map_rows,
        "config": {
            "materiality_pct": materiality_pct,
            "materiality_amount_sar": int(materiality_amount_sar),
            "bilingual": bool(bilingual),
            "enforce_no_speculation": bool(enforce_no_speculation),
        },
    }


@app.post("/drafts/upload", response_model=List[DraftResponse])
async def drafts_upload(
    budget_actuals: UploadFile = File(..., description="Budget–Actuals CSV/XLS/XLSX"),
    change_orders: UploadFile = File(..., description="Change Orders CSV/XLS/XLSX"),
    vendor_map: UploadFile = File(..., description="Vendor Map CSV/XLS/XLSX"),
    category_map: UploadFile = File(..., description="Category Map CSV/XLS/XLSX"),
    materiality_pct: float = Form(5),
    materiality_amount_sar: float = Form(100000),
    bilingual: bool = Form(True),
    enforce_no_speculation: bool = Form(True),
):
    try:
        ba_df = _read_tabular(await budget_actuals.read(), budget_actuals.filename)
        co_df = _read_tabular(await change_orders.read(), change_orders.filename)
        vm_df = _read_tabular(await vendor_map.read(), vendor_map.filename)
        cm_df = _read_tabular(await category_map.read(), category_map.filename)

        payload = _build_payload(
            _df_to_records(ba_df),
            _df_to_records(co_df),
            _df_to_records(vm_df),
            _df_to_records(cm_df),
            materiality_pct,
            materiality_amount_sar,
            bilingual,
            enforce_no_speculation,
        )
        req = DraftRequest(**payload)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse files: {e}")

    try:
        return create_drafts(req)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Draft generation failed: {e}")


# --- Single Data File endpoint (extended) ---


@app.post("/singlefile/report")
async def singlefile_report(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    If Budget+Actual found -> {"mode":"variance","variances":[...], "insights": {...}}
    Else -> {"mode":"summary","items":[...,"drafts":[{en,ar},...] ]}
    """
    data = await file.read()
    res = process_single_file(file.filename or "upload.bin", data)
    if res.get("mode") == "summary":
        items = res.get("items", [])
        res["drafts"] = [
            draft_bilingual_procurement_card(it, "Uploaded procurement file")
            for it in items
        ]
    return res


@app.post("/singlefile/analyze")
async def analyze_single_file_endpoint(
    file: UploadFile = File(...),
    bilingual: bool = Form(True),
    no_speculation: bool = Form(True),
) -> Dict[str, Any]:
    """
    Strict single-file analysis:
    - If we detect budget/actual pairs => produce variance_insights.
    - Otherwise => produce procurement_summary cards only.
    - Never invent numbers; only paraphrase descriptions if bilingual.
    """
    data = await file.read()
    return await analyze_single_file(
        data,
        file.filename,
        bilingual=bilingual,
        no_speculation=no_speculation,
    )


@app.post("/drafts/from-file")
async def drafts_from_file(
    file: UploadFile = File(...),
    bilingual: bool = Form(True),
    no_speculation: bool = Form(True),
    materiality_pct: float = Form(5.0),
    materiality_amount_sar: float = Form(100000.0),
):
    data = await file.read()
    parsed = parse_single_file(file.filename, data)

    # If we found variance items, return them as-is (UI already knows how to render)
    if "variance_items" in parsed:
        return JSONResponse(parsed)

    # Otherwise, return Procurement Summary skeleton (UI will render dedicated cards)
    if "procurement_summary" in parsed:
        return JSONResponse(parsed)

    return JSONResponse({"procurement_summary": {"items": [], "meta": {}}})
