
import os
import io
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
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

app: FastAPI = FastAPI(title="Oaktree Variance Drafts API", version="0.1.0")

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
    return FileResponse("app/static/ui.html")

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

    # Read CSVs
    ba = await budget_actuals.read()
    co = await change_orders.read()
    vm = await vendor_map.read()
    cm = await category_map.read()
    df_ba = pd.read_csv(io.BytesIO(ba)).fillna("")
    df_co = pd.read_csv(io.BytesIO(co)).fillna("")
    df_vm = pd.read_csv(io.BytesIO(vm)).fillna("")
    df_cm = pd.read_csv(io.BytesIO(cm)).fillna("")

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
    """Parse CSV uploads into a JSON DraftRequest payload."""
    if pd is None:
        return JSONResponse(
            {"error": "pandas is not installed on the server"}, status_code=500
        )

    async def to_records(upload: UploadFile) -> List[Dict[str, Any]]:
        content = await upload.read()
        try:
            df = pd.read_csv(io.StringIO(content.decode("utf-8")))
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(content))
        df.columns = [str(c).strip() for c in df.columns]
        return df.to_dict(orient="records")

    payload = {
        "budget_actuals": await to_records(budget_actuals),
        "change_orders": await to_records(change_orders),
        "vendor_map": await to_records(vendor_map),
        "category_map": await to_records(category_map),
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
