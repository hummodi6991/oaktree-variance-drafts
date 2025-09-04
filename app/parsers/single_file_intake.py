from typing import Dict, Any, List
import io
import pandas as pd
from docx import Document
from app.utils.diagnostics import DiagnosticContext
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
  with DiagnosticContext(file_name=filename, file_size=len(data)) as diag:
    diag.step("start", filename=name)
    if name.endswith(".pdf"):
      diag.step("parse_pdf_start")
      ps = parse_procurement_pdf(data)
      diag.step("parse_pdf_success", items=len(ps.get("items", [])))
      return {"procurement_summary": ps, "diagnostics": diag.to_dict()}
    if name.endswith(".docx"):
      diag.step("parse_docx_start")
      doc = Document(io.BytesIO(data))
      text = "\n".join(p.text for p in doc.paragraphs)
      diag.step("parse_docx_success", paragraphs=len(doc.paragraphs))
      return {"procurement_summary": {"items":[{"item_code": None, "description": text[:2000], "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"}], "meta":{}}, "diagnostics": diag.to_dict()}
    if name.endswith(".csv"):
      diag.step("parse_csv_start")
      try:
        df = pd.read_csv(io.BytesIO(data))
        diag.step("parse_csv_success", rows=int(df.shape[0]), cols=int(df.shape[1]))
      except Exception as e:
        diag.error("read_csv_failed", e)
        return {"error": "failed_to_read_csv", "diagnostics": diag.to_dict()}
      df = _map_cols(df)
    elif name.endswith(".xlsx") or name.endswith(".xls"):
      diag.step("parse_excel_start")
      try:
        sheets = pd.read_excel(io.BytesIO(data), sheet_name=None)
        diag.step("parse_excel_success", sheets=list(sheets.keys()))
      except Exception as e:
        diag.error("read_excel_failed", e)
        return {"error": "failed_to_read_excel", "diagnostics": diag.to_dict()}
      frames: List[pd.DataFrame] = []
      for sn, sh in sheets.items():
        mapped = _map_cols(sh)
        has_ba = _has_budget_actual(mapped)
        diag.step("sheet_loaded", sheet=sn, rows=int(sh.shape[0]), cols=int(sh.shape[1]), has_budget_actual=has_ba)
        if has_ba:
          frames.append(mapped)
      if frames:
        df = pd.concat(frames, ignore_index=True)
      else:
        first = next(iter(sheets.values()), pd.DataFrame())
        df = _map_cols(first)
    else:
      diag.step("parse_text_start")
      text = data.decode("utf-8", errors="ignore")
      diag.step("parse_text_success", chars=len(text))
      return {"procurement_summary": {"items":[{"item_code": None, "description": text[:2000], "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"}], "meta":{}}, "diagnostics": diag.to_dict()}

    df = _map_cols(df)
    diag.step("columns_mapped", columns=list(df.columns))
    if _has_budget_actual(df):
      diag.step("mode_variance", rows=int(df.shape[0]))
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
      return {"variance_items": records, "diagnostics": diag.to_dict()}
    else:
      diag.step("mode_procurement_summary")
      items: List[Dict[str,Any]] = []
      for _, r in df.head(50).iterrows():
        desc = " ".join(str(v) for v in r.to_dict().values() if pd.notna(v))[:2000]
        items.append({"item_code": None, "description": desc, "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"})
      return {"procurement_summary": {"items": items, "meta": {}}, "diagnostics": diag.to_dict()}
