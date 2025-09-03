from typing import Dict, Any, List
import io
import pandas as pd
from docx import Document
from .procurement_pdf import parse_procurement_pdf

# Column synonym maps for tolerant CSV/Excel intake
MAP = {
  "budget": {"budget","budget_sar","planned","plan","budget_value","planned_sar"},
  "actual": {"actual","actuals","spent","spend","actual_sar","cost_to_date","ctd"},
  "project_id": {"project","project_id","job","job_id","project name"},
  "period": {"period","month","date"},
  "cost_code": {"cost_code","code","account","gl","linked_cost_code"},
  "category": {"category","cat","trade"}
}

def _map_cols(df: pd.DataFrame) -> pd.DataFrame:
  lower = {c: c.strip().lower() for c in df.columns}
  rename = {}
  inv = {k: set(v) for k,v in MAP.items()}
  for std, alts in inv.items():
    for col, low in lower.items():
      if low in alts and std not in df.columns:
        rename[col] = std
  if rename:
    df = df.rename(columns=rename)
  return df

def _has_budget_actual(df: pd.DataFrame) -> bool:
  cols = {c.lower() for c in df.columns}
  return bool(MAP["budget"] & cols) and bool(MAP["actual"] & cols)

def parse_single_file(filename: str, data: bytes) -> Dict[str, Any]:
  name = (filename or "").lower()
  # PDF
  if name.endswith(".pdf"):
    return {"procurement_summary": parse_procurement_pdf(data)}
  # Word
  if name.endswith(".docx"):
    doc = Document(io.BytesIO(data))
    text = "\n".join(p.text for p in doc.paragraphs)
    return {"procurement_summary": {"items":[{"item_code": None, "description": text[:2000], "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"}], "meta":{}}}
  # CSV/Excel -> try variance; if not possible, summarize rows
  if name.endswith(".csv"):
    df = pd.read_csv(io.BytesIO(data))
  elif name.endswith(".xlsx") or name.endswith(".xls"):
    df = pd.read_excel(io.BytesIO(data))
  else:
    # plain text fallback
    text = data.decode("utf-8", errors="ignore")
    return {"procurement_summary": {"items":[{"item_code": None, "description": text[:2000], "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"}], "meta":{}}}

  df = _map_cols(df)
  if _has_budget_actual(df):
    # minimal variance calc; UI will render nicely
    df["variance_sar"] = df.filter(like="actual", axis=1).iloc[:,0] - df.filter(like="budget", axis=1).iloc[:,0]
    bud = df.filter(like="budget", axis=1).iloc[:,0].abs().replace(0, pd.NA)
    df["variance_pct"] = (df["variance_sar"] / bud * 100).round(2)
    records = []
    for _, r in df.iterrows():
      records.append({
        "variance": {
          "project_id": r.get("project_id"),
          "period": str(r.get("period")),
          "category": r.get("category"),
          "budget_sar": float(r.filter(like="budget").iloc[0]) if r.filter(like="budget").size else None,
          "actual_sar": float(r.filter(like="actual").iloc[0]) if r.filter(like="actual").size else None,
          "variance_sar": float(r.get("variance_sar")) if pd.notna(r.get("variance_sar")) else None,
          "variance_pct": float(r.get("variance_pct")) if pd.notna(r.get("variance_pct")) else None,
          "drivers": [],
          "vendors": [],
          "evidence_links": []
        },
        "draft_en": None,
        "draft_ar": None,
        "analyst_notes": None
      })
    return {"variance_items": records}
  else:
    # not a variance file → list first 50 rows as procurement-like lines (no invention)
    items: List[Dict[str,Any]] = []
    for _, r in df.head(50).iterrows():
      desc = " ".join(str(v) for v in r.to_dict().values() if pd.notna(v))[:2000]
      items.append({"item_code": None, "description": desc, "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"})
    return {"procurement_summary": {"items": items, "meta": {}}}
