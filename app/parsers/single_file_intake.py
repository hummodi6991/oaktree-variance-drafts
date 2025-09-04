from typing import Dict, Any, List, Optional
import io
import pandas as pd
from docx import Document
from app.utils.diagnostics import DiagnosticContext
from .procurement_pdf import parse_procurement_pdf
import re
import pdfplumber

# Column synonym maps for tolerant CSV/Excel intake
MAP = {
  "budget": {"budget","budget_sar","planned","plan","budget_value","planned_sar"},
  "actual": {"actual","actuals","spent","spend","actual_sar","cost_to_date","ctd"},
  "label": {"label","item","description","cost_code","category","project_id","name"}
}

def _map_cols(df: pd.DataFrame) -> pd.DataFrame:
  lower = {c: c.strip().lower() for c in df.columns}
  rename: Dict[str, str] = {}
  used = {c.lower() for c in df.columns}
  for std, alts in MAP.items():
    for col, low in lower.items():
      if low in alts and std not in used:
        rename[col] = std
        used.add(std)
  if rename:
    df = df.rename(columns=rename)
  return df

def _has_budget_actual(df: pd.DataFrame) -> bool:
  cols = {c.lower() for c in df.columns}
  return bool(MAP["budget"] & cols) and bool(MAP["actual"] & cols)

def _strip_num(x: Any) -> Optional[float]:
  try:
    if x is None:
      return None
    s = str(x)
    s = re.sub(r"[^\d\.\-]", "", s).strip()
    return float(s) if s not in ("", "-", ".", "-.") else None
  except Exception:
    return None

def _emit_variance_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
  """Given a DF that already has 'budget' and 'actual' standardized, emit variance rows."""
  df = df.copy()
  label_col = next((c for c in ["label","item","description","cost_code","category","project_id"] if c in df.columns), None)
  bcol = next((c for c in ["budget","budget_sar"] if c in df.columns), None)
  acol = next((c for c in ["actual","actuals","actual_sar","spent","spend","cost_to_date","ctd"] if c in df.columns), None)
  if not (bcol and acol):
    return []
  df[bcol] = df[bcol].apply(_strip_num)
  df[acol] = df[acol].apply(_strip_num)
  out: List[Dict[str, Any]] = []
  for _, r in df.iterrows():
    b = r.get(bcol)
    a = r.get(acol)
    if b is None and a is None:
      continue
    try:
      out.append({
        "label": str(r.get(label_col) or r.get("item") or r.get("description") or "Line"),
        "budget_sar": b,
        "actual_sar": a,
        "variance_sar": (a - b) if (a is not None and b is not None) else None,
      })
    except Exception:
      continue
  return out

def parse_single_file(filename: str, data: bytes) -> Dict[str, Any]:
  name = (filename or "").lower()
  with DiagnosticContext(file_name=filename, file_size=len(data)) as diag:
    diag.step("start", filename=name)
    if name.endswith(".pdf"):
      # New: attempt Budget/Actual detection from PDF tables first
      diag.step("parse_pdf_start")
      variance_rows: List[Dict[str, Any]] = []
      try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
          tables_scanned = 0
          for page in pdf.pages:
            tbls = page.extract_tables() or []
            for t in tbls:
              tables_scanned += 1
              try:
                df = pd.DataFrame(t[1:], columns=[str(c).strip() for c in t[0]])
              except Exception:
                continue
              df = _map_cols(df)
              if _has_budget_actual(df):
                variance_rows.extend(_emit_variance_rows(df))
          diag.step("pdf_tables_scanned", tables=int(tables_scanned), variance_rows=int(len(variance_rows)))
      except Exception as e:
        diag.warn("pdf_table_scan_failed", error=str(e))

      if variance_rows:
        diag.step("mode_variance_pdf", rows=int(len(variance_rows)))
        return {"variance_items": variance_rows, "diagnostics": diag.to_dict()}

      # Fallback: procurement summary extraction from existing PDF parser
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
      rows = _emit_variance_rows(df)
      diag.step("mode_variance", rows=int(len(rows)))
      return {"variance_items": rows, "diagnostics": diag.to_dict()}
    else:
      diag.step("mode_procurement_summary")
      items: List[Dict[str,Any]] = []
      for _, r in df.head(50).iterrows():
        desc = " ".join(str(v) for v in r.to_dict().values() if pd.notna(v))[:2000]
        items.append({"item_code": None, "description": desc, "qty": None, "unit_price_sar": None, "amount_sar": None, "vendor_name": None, "doc_date": None, "source":"uploaded_file"})
      return {"procurement_summary": {"items": items, "meta": {}}, "diagnostics": diag.to_dict()}
