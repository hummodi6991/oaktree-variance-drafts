
import os
import io
from fastapi import FastAPI, HTTPException, Security, File, UploadFile, Form
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List, Optional
import pandas as pd
from .schemas import DraftRequest, DraftResponse
from .pipeline import build_category_lookup, group_variances, attach_drivers_and_vendors, filter_materiality
from .gpt_client import generate_draft

app = FastAPI(title="Oaktree Variance Drafts API", version="0.1.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# --- API key security (adds "Authorize" in Swagger) --------------------------
api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)

def require_api_key(x_api_key: str = Security(api_key_header)) -> None:
    expected = os.getenv("API_KEY")
    if not expected or not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
# -----------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ui", include_in_schema=False)
def ui():
    return FileResponse(os.path.join("app", "static", "index.html"))

@app.post("/drafts", response_model=List[DraftResponse])
def create_drafts(req: DraftRequest, _=Security(require_api_key)):
    """
    Create EN/AR variance explanation drafts.
    """
    # existing implementation continues…
    cat_lu = build_category_lookup(req.category_map)
    items = group_variances(req.budget_actuals, cat_lu)
    attach_drivers_and_vendors(items, req.change_orders, req.vendor_map, cat_lu)
    material = filter_materiality(items, req.config)
    out = []
    for v in material:
        en, ar = generate_draft(v, req.config)
        out.append(DraftResponse(variance=v, draft_en=en, draft_ar=ar or None))
    return out


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
        DraftRequest,
        BudgetActualRow,
        ChangeOrderRow,
        VendorMapRow,
        CategoryMapRow,
        ConfigModel,
    )
    from app.pipeline import generate_drafts

    expected = os.getenv("API_KEY")
    if expected and api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    # Read CSVs
    ba = await budget_actuals.read()
    co = await change_orders.read()
    vm = await vendor_map.read()
    cm = await category_map.read()
    df_ba = pd.read_csv(io.BytesIO(ba))
    df_co = pd.read_csv(io.BytesIO(co))
    df_vm = pd.read_csv(io.BytesIO(vm))
    df_cm = pd.read_csv(io.BytesIO(cm))

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

    result = await generate_drafts(req)
    return result
