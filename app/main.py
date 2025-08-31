
import os
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List
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
