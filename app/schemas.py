
"""Pydantic data models for the Variance Drafts service."""

from pydantic import BaseModel, Field
from typing import List, Optional

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
