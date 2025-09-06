
import os
import io
import uuid
import csv  # noqa: F401
import json  # noqa: F401
import statistics
from typing import Any, Dict, List, Optional
import re
import textwrap
import time
import asyncio
import platform
import logging

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
    Request,
)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel

from .schemas import (
    DraftRequest,
    ProcurementItem,
    VendorSnapshot,
    DraftsOrSummary,
)
from .pipeline import generate_drafts
from .services.csv_loader import parse_tabular
from app.services.singlefile import process_single_file
from .llm.extract_from_text import extract_items_via_llm
from app.parsers.single_file import analyze_single_file
from app.services.insights import compute_procurement_insights, summarize_procurement_lines
from app.gpt_client import summarize_financials
from openai_client_helper import build_client
from app.routers import drafts as drafts_router
from app.utils.local import is_local_only

app: FastAPI = FastAPI(title="Oaktree Variance Drafts API", version="0.1.0")

logger = logging.getLogger("uvicorn.error")


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = rid
        start = time.time()
        try:
            resp = await call_next(request)
            return resp
        except Exception as e:  # pragma: no cover - defensive
            logger.exception("Unhandled error [%s] %s", rid, request.url.path)
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "error": "internal_error",
                    "detail": str(e)[:400],
                    "stage": "unhandled_exception",
                    "request_id": rid,
                },
            )
        finally:
            dur = int((time.time() - start) * 1000)
            logger.info("%s %s %s %sms", rid, request.method, request.url.path, dur)


app.add_middleware(RequestContextMiddleware)


def _mask(val: str | None, keep: int = 8):
    if not val:
        return None
    return (val[:keep] + "…") if len(val) > keep else val


@app.get("/diag/health")
def diag_health(request: Request):
    return {
        "ok": True,
        "request_id": request.state.request_id,
        "runtime": {"python": platform.python_version()},
        "env": {
            "OPENAI_MODEL": os.getenv("OPENAI_MODEL"),
            "OPENAI_BASE_URL": os.getenv("OPENAI_BASE_URL"),
            "REQUIRE_API_KEY": os.getenv("REQUIRE_API_KEY", "true"),
        },
        "version": os.getenv("GIT_COMMIT", "unknown"),
    }


@app.get("/diag/openai")
def diag_openai(request: Request):
    key = os.getenv("OPENAI_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
    ok = bool(key)
    err: str | None = None
    if ok:
        try:
            client = build_client()
            client.responses.create(
                model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                input="ping",
            )
        except Exception as e:  # pragma: no cover - diagnostics
            ok = False
            err = repr(e)
    return {
        "ok": ok,
        "request_id": request.state.request_id,
        "key_prefix": _mask(key),
        "endpoint": os.getenv("OPENAI_BASE_URL") or "https://api.openai.com",
        "model": os.getenv("OPENAI_MODEL"),
        "error": err,
    }


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
async def extract_freeform(
    request: Request,
    files: List[UploadFile] = File(...),
) -> Dict[str, Any]:
    """Accept CSV/XLSX/DOCX/PDF/TXT and return tolerant row dicts.

    The return payload includes a ``mode`` field indicating whether the rows
    resemble change-order data (``change_orders``) or budget-vs-actual data
    (``budget_actuals``). This allows the caller to route the rows to the
    appropriate pipeline.
    """
    # LLM-only mode: ignore local_only header; fail if LLM fails
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
                from app.utils.retries import retry_iter
                for it in retry_iter(_extract_rows_via_llm, text):
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
            if not rows:
                raise HTTPException(status_code=502, detail="LLM extraction produced no rows.")
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
                    from app.utils.retries import retry_iter
                    for it in retry_iter(_extract_rows_via_llm, text):
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
                if not rows:
                    raise HTTPException(status_code=502, detail="LLM extraction produced no rows.")
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

app.include_router(drafts_router.router, dependencies=deps)

@app.get("/health")
def health():
    return {"status": "ok"}


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
    request: Request, req: DraftRequest, background_tasks: BackgroundTasks
):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "result": None,
    }

    local_only = is_local_only(request)

    def run_job():
        try:
            _set_job(job_id, status="running", progress=5, message="Validating input")

            def cb(pct: int, msg: str = ""):
                _set_job(
                    job_id,
                    progress=max(0, min(100, int(pct))),
                    message=msg or jobs[job_id].get("message"),
                )

            result, meta = generate_drafts(req, progress_cb=cb, force_local=local_only)
            payload = {
                "variances": result,
                "_meta": meta.model_dump(),
            } if isinstance(result, list) else {**result, "_meta": meta.model_dump()}
            _set_job(
                job_id,
                status="done",
                progress=100,
                message="Completed",
                result=payload,
            )
            logger.info(
                "drafts_async llm_used=%s model=%s forced_local=%s",
                meta.llm_used,
                meta.model,
                meta.forced_local,
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

@app.post("/drafts", response_model=DraftsOrSummary, dependencies=deps)
def create_drafts(request: Request, req: DraftRequest):
    """Create EN/AR variance explanation drafts."""
    local_only = is_local_only(request)
    result, meta = generate_drafts(req, force_local=local_only)
    logger.info(
        "drafts llm_used=%s model=%s forced_local=%s",
        meta.llm_used,
        meta.model,
        meta.forced_local,
    )
    if isinstance(result, list):
        return {"variances": result, "_meta": meta.model_dump()}
    result["_meta"] = meta.model_dump()
    return result


@app.post("/upload", include_in_schema=False)
async def upload(
    request: Request,
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
    local_only = is_local_only(request)

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
            # Always LLM-aided extraction for non-tabular files; no fallbacks.
            text = _text_from_bytes(name, data)
            ba_text = _rows_from_budget_actuals_text(text)
            if ba_text:
                rows = ba_text
                mode = "budget_actuals"
            else:
                rows = _rows_from_text(text)
                if not rows:
                    from app.utils.retries import retry_iter
                    out: List[Dict[str, Any]] = []
                    try:
                        for it in retry_iter(_extract_rows_via_llm, text):
                            out.append(
                                {
                                    "project_id": None,
                                    "linked_cost_code": None,
                                    "description": it.get("description"),
                                    "file_link": None,
                                    "co_id": it.get("co_id"),
                                    "date": None,
                                    "amount_sar": it.get("amount_sar"),
                                    "vendor_name": None,
                                }
                            )
                    except Exception:
                        out = []
                    rows = out
                if not rows:
                    summary = textwrap.shorten((text or "").strip(), width=200, placeholder="...")
                    return {"mode": "summary", "summary": summary}
        filtered = [
            r for r in rows if any(v is not None and str(v).strip() != "" for v in r.values())
        ]
        if not filtered:
            if (text or "").strip():
                summary = textwrap.shorten((text or "").strip(), width=200, placeholder="...")
                return {"mode": "summary", "summary": summary}
            return {"mode": "summary", "summary": ""}
        has_amount = any(
            (r.get("amount_sar") is not None)
            or (r.get("budget_sar") is not None)
            or (r.get("actual_sar") is not None)
            for r in filtered
        )
        if not has_amount:
            if (text or "").strip():
                summary = textwrap.shorten((text or "").strip(), width=200, placeholder="...")
                return {"mode": "summary", "summary": summary}
            return {"mode": "summary", "summary": ""}

        if mode == "budget_actuals":
            paired = [
                r
                for r in filtered
                if r.get("budget_sar") is not None and r.get("actual_sar") is not None
            ]
            if paired:
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
                    drafts, meta = generate_drafts(req, force_local=local_only)
                    logger.info(
                        "upload llm_used=%s model=%s forced_local=%s",
                        meta.llm_used,
                        meta.model,
                        meta.forced_local,
                    )

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
                    "_meta": meta.model_dump(),
                    "paired_count": len(paired),
                    "unpaired_count": len(unpaired),
                    "unpaired_rows": unpaired,
                    "unpaired_summary": unpaired_summary,
                }
            # No paired rows -> summarize instead of returning cards
            analysis = compute_procurement_insights(filtered)
            summary_data = summarize_procurement_lines(filtered)
            summary_text = summarize_financials(summary_data, analysis)
            return {
                "mode": "summary",
                "summary": summary_text,
                "analysis": analysis,
                "insights": analysis,
            }

        # No budget/actual pairs detected -> summarize the procurement lines
        analysis = compute_procurement_insights(filtered)
        summary_data = summarize_procurement_lines(filtered)
        summary_text = summarize_financials(summary_data, analysis)
        return {
            "mode": "summary",
            "summary": summary_text,
            "analysis": analysis,
            "insights": analysis,
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

    result, meta = generate_drafts(req, force_local=local_only)
    logger.info(
        "upload llm_used=%s model=%s forced_local=%s",
        meta.llm_used,
        meta.model,
        meta.forced_local,
    )
    if isinstance(result, list):
        return result
    result["_meta"] = meta.model_dump()
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


@app.post("/drafts/upload", response_model=DraftsOrSummary)
async def drafts_upload(
    request: Request,
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
        return create_drafts(request, req)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Draft generation failed: {e}")


# --- Single Data File endpoint (extended) ---


@app.post("/singlefile/report")
async def singlefile_report(request: Request, file: UploadFile = File(...)) -> Dict[str, Any]:
    """Return summary/analysis/insights for a single uploaded file."""
    data = await file.read()
    local_only = is_local_only(request)
    res = await asyncio.to_thread(
        process_single_file, file.filename or "upload.bin", data, local_only=local_only
    )
    meta = res.get("_meta", {})
    logger.info(
        "singlefile_report llm_used=%s model=%s forced_local=%s",
        meta.get("llm_used"),
        meta.get("model"),
        meta.get("forced_local"),
    )
    return {"kind": "insights", **res}


@app.post("/singlefile/analyze")
async def analyze_single_file_endpoint(
    request: Request,
    file: UploadFile = File(...),
    bilingual: bool = Form(True),
    no_speculation: bool = Form(True),
) -> Dict[str, Any]:
    """Analyze a single file by delegating to ChatGPT."""
    data = await file.read()
    local_only = is_local_only(request)
    res = await analyze_single_file(
        data,
        file.filename,
        bilingual=bilingual,
        no_speculation=no_speculation,
        local_only=local_only,
    )
    meta = res.get("_meta", {})
    logger.info(
        "singlefile_analyze llm_used=%s model=%s forced_local=%s",
        meta.get("llm_used"),
        meta.get("model"),
        meta.get("forced_local"),
    )
    return res


# ---------------- In-memory job store (lightweight) ------------------------
JOBS: dict[str, dict] = {}


def jobs_put(jid: str, **k):
    d = JOBS.setdefault(jid, {"status": "queued"})
    d.update(k)
    JOBS[jid] = d
    return d


@app.get("/jobs/{job_id}")
def jobs_get(job_id: str, request: Request):
    j = JOBS.get(job_id)
    if not j:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": "job_not_found", "request_id": request.state.request_id},
        )
    j["request_id"] = request.state.request_id
    return j


@app.post("/singlefile/generate-async")
async def generate_from_file_async(request: Request, file: UploadFile = File(...)):
    rid = getattr(request.state, "request_id", str(uuid.uuid4()))
    if not file:
        raise HTTPException(status_code=400, detail="no_file")
    name = (file.filename or "").lower()
    raw = await file.read()
    if not raw:
        return {"ok": False, "error": "empty_file", "stage": "upload", "request_id": rid}

    import mimetypes

    mime = file.content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"

    job_id = str(uuid.uuid4())
    jobs_put(
        job_id,
        ok=False,
        status="queued",
        started_at=time.time(),
        file=name,
        mime=mime,
        stage="queued",
    )

    local_only = is_local_only(request)

    async def _worker():
        t0 = time.time()
        try:
            jobs_put(job_id, status="parsing", stage="parsing")
            result = await asyncio.to_thread(
                process_single_file, name, raw, local_only=local_only
            )
            meta = result.get("_meta", {})
            jobs_put(
                job_id,
                ok=True,
                status="done",
                stage="done",
                timings_ms={"total": int((time.time() - t0) * 1000)},
                payload=result,
            )
            logger.info(
                "singlefile_generate_async llm_used=%s model=%s forced_local=%s",
                meta.get("llm_used"),
                meta.get("model"),
                meta.get("forced_local"),
            )
        except ValueError as ve:
            jobs_put(
                job_id,
                ok=False,
                status="error",
                stage="validation",
                error="validation_error",
                detail=str(ve),
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.exception("Single-file job failed [%s]", job_id)
            jobs_put(
                job_id,
                ok=False,
                status="error",
                stage="processing",
                error="processing_failed",
                detail=str(e)[:400],
            )

    asyncio.create_task(_worker())
    return {"ok": True, "job_id": job_id, "request_id": rid}
