
import os
import io
import uuid
import csv  # noqa: F401
import json  # noqa: F401
from typing import Any, Dict, List, Optional

import chardet
import pandas as pd
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except Exception:  # pragma: no cover - optional dependency
    pdf_extract_text = None
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

from .schemas import DraftRequest, DraftResponse
from .pipeline import generate_drafts
from .services.csv_loader import parse_tabular
from .parsers.procurement_pdf import parse_procurement_pdf
from .llm.extract_from_text import extract_items_via_llm

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
    if low.endswith(".pdf") and pdf_extract_text:
        try:
            return pdf_extract_text(io.BytesIO(data)) or ""
        except Exception:
            return ""
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
        out.append(item)
    return out


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


@app.post("/extract/freeform")
async def extract_freeform(files: List[UploadFile] = File(...)) -> Dict[str, Any]:
    """Accept CSV/XLSX/DOCX/PDF/TXT and return tolerant change-order like rows."""
    all_rows: List[Dict[str, Any]] = []
    for f in files:
        data = await f.read()
        name = f.filename or "upload.bin"
        low = name.lower()
        rows: List[Dict[str, Any]] = []
        if any(low.endswith(ext) for ext in [".csv", ".xlsx", ".xls"]):
            df = _df_from_bytes(name, data)
            if not df.empty:
                rows = _rows_from_tablelike(df)
        elif any(low.endswith(ext) for ext in [".pdf", ".docx", ".txt", ".md", ".rtf"]):
            text = _text_from_bytes(name, data)
            rows = _rows_from_text(text)
        else:
            df = _df_from_bytes(name, data)
            if not df.empty:
                rows = _rows_from_tablelike(df)
            else:
                text = _text_from_bytes(name, data)
                rows = _rows_from_text(text)
        all_rows.extend(rows)
    filtered = [r for r in all_rows if (r.get("description") or r.get("amount_sar") is not None)]
    return {"rows": filtered, "count": len(filtered)}

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


# Procurement PDF extraction
@app.post("/extract/procurement")
async def extract_procurement(files: list[UploadFile] = File(...)) -> dict:
    """
    Accepts one or more procurement PDFs. Returns structured rows suitable
    for change_orders. No fabrication: fields not present remain null.
    """
    results = []
    for f in files:
        data = await f.read()
        file_url = None  # If you store uploads, set a URL here
        parsed = parse_procurement_pdf(data, file_url=file_url)
        rows = parsed["rows"]

        # Fallback to LLM if deterministic rows look empty
        if not rows or all(not r.get("description") and not r.get("amount_sar") for r in rows):
            llm_rows = extract_items_via_llm(parsed["raw_preview"])
            for it in llm_rows:
                rows.append({
                    "project_id": None,
                    "linked_cost_code": None,
                    "description": it.get("description"),
                    "file_link": file_url,
                    "co_id": it.get("co_id"),
                    "date": parsed["meta"].get("doc_date"),
                    "amount_sar": it.get("amount_sar"),
                    "vendor_name": parsed["meta"].get("vendor_name"),
                    "qty": it.get("qty"),
                    "unit_price_sar": it.get("unit_price_sar"),
                    "source": "procurement_pdf_llm"
                })
        results.append({"filename": f.filename, "meta": parsed["meta"], "rows": rows})
    return {"ok": True, "documents": results}


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
    budget_actuals: UploadFile = File(...),
    change_orders: UploadFile = File(...),
    vendor_map: UploadFile = File(...),
    category_map: UploadFile = File(...),
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

    # Read CSV or Excel files
    ba = await budget_actuals.read()
    co = await change_orders.read()
    vm = await vendor_map.read()
    cm = await category_map.read()
    df_ba = _read_tabular(ba, budget_actuals.filename).fillna("")
    df_co = _read_tabular(co, change_orders.filename).fillna("")
    df_vm = _read_tabular(vm, vendor_map.filename).fillna("")
    df_cm = _read_tabular(cm, category_map.filename).fillna("")

    # Convert rows to pydantic models
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
