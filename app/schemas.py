
"""Pydantic data models for the Variance Drafts service."""

from pydantic import BaseModel, Field, AliasChoices
from typing import List, Optional, Any, Dict, Union
from typing_extensions import Literal

class BudgetActualRow(BaseModel):
    project_id: str
    period: str  # 'YYYY-MM'
    cost_code: str
    category: Optional[str] = None  # If not provided, will be mapped using CategoryMapRow
    budget_sar: float
    actual_sar: float
    currency: Optional[str] = "SAR"
    remarks: Optional[str] = None

class ChangeOrderRow(BaseModel):
    project_id: Optional[str] = None
    co_id: Optional[str] = None            # was required → now optional
    date: Optional[str] = None             # allow raw string date if that's what's in file/PDF
    amount_sar: Optional[float] = None     # was required → now optional
    category: Optional[str] = None
    description: Optional[str] = None
    linked_cost_code: Optional[str] = None
    file_link: Optional[str] = None

class VendorMapRow(BaseModel):
    project_id: str
    cost_code: str
    vendor_name: str
    trade: Optional[str] = None
    contract_id: Optional[str] = None

class CategoryMapRow(BaseModel):
    cost_code: str
    category: str  # One of: Materials, Labor, Consultants, Overhead

class ConfigModel(BaseModel):
    materiality_pct: float = Field(default=5.0, description="Explain |variance_pct| >= this value")
    materiality_amount_sar: float = Field(default=100_000.0, description="Explain |variance_amount| >= this value")
    bilingual: bool = True
    enforce_no_speculation: bool = True

class VarianceItem(BaseModel):
    project_id: str
    period: str
    category: str
    budget_sar: float
    actual_sar: float
    variance_sar: float
    variance_pct: float
    drivers: List[str] = Field(default_factory=list)
    vendors: List[str] = Field(default_factory=list)
    evidence_links: List[str] = Field(default_factory=list)

class DraftRequest(BaseModel):
    budget_actuals: List[BudgetActualRow]
    change_orders: List[ChangeOrderRow] = Field(default_factory=list)
    vendor_map: List[VendorMapRow] = Field(default_factory=list)
    category_map: List[CategoryMapRow] = Field(default_factory=list)
    config: ConfigModel = Field(default_factory=ConfigModel)

class DraftResponse(BaseModel):
    variance: VarianceItem
    draft_en: str
    draft_ar: Optional[str] = None
    analyst_notes: Optional[str] = None


class SummaryResponse(BaseModel):
    kind: Literal["summary"] = "summary"
    message: str
    insights: Dict[str, Any] = Field(default_factory=dict)

# Envelope used by /drafts to return the list together with metadata
class DraftsEnvelope(BaseModel):
    variances: List[DraftResponse]
    meta: Optional[Dict[str, Any]] = Field(default=None, alias="_meta")

    model_config = {
        "populate_by_name": True,
    }

# Endpoints may return either the wrapped list of drafts or a summary
DraftsOrSummary = Union[DraftsEnvelope, SummaryResponse]


class TokenUsage(BaseModel):
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None


class GenerationMeta(BaseModel):
    llm_used: bool
    provider: Optional[Literal["OpenAI"]] = None
    model: Optional[str] = None
    token_usage: Optional[TokenUsage] = None
    forced_local: Optional[bool] = None

class ProcurementItem(BaseModel):
    """Lightweight summary card for single-file procurement uploads."""

    item_code: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[float] = None
    unit_price: Optional[float] = None
    amount_sar: Optional[float] = None
    vendor: Optional[str] = None
    document_date: Optional[str] = None
    evidence_link: str = "Uploaded procurement file"
    draft_en: str
    draft_ar: Optional[str] = None


class VendorSnapshot(BaseModel):
    """Aggregated vendor-level info for single-file uploads."""

    vendor: str
    quote_date: Optional[str] = None
    validity_window: Optional[str] = None
    delivery_lead_time: Optional[str] = None
    payment_terms: Optional[str] = None
    currency: Optional[str] = None
    vat_rate: Optional[float] = None
    total_excl_vat: Optional[float] = None
    total_incl_vat: Optional[float] = None
