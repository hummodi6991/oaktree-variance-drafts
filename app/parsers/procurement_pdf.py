import io
import re
from typing import List, Dict, Any
import pdfplumber

# Simple helpers
_RE_MONEY = r'(?:SAR|SR|\$)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)'
_RE_DATE  = r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{2}[/-]\d{2})'
_RE_ITEM  = r'\b(D0?\d+)\b'  # e.g., D01, D02...

def _clean_num(x: str) -> float:
    x = x.replace(',', '').strip()
    try:
        return float(x)
    except Exception:
        return None

def _first(pats: List[re.Pattern], text: str) -> str|None:
    for p in pats:
        m = p.search(text)
        if m:
            return m.group(1).strip()
    return None

def pdf_to_text_pages(data: bytes) -> List[str]:
    texts = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            # prefer tables->text if available
            page_text = page.extract_text(x_tolerance=1.5, y_tolerance=3.0) or ""
            texts.append(page_text)
    return texts

def greedy_line_items(all_text: str) -> List[Dict[str, Any]]:
    """
    Try to pull item blocks with code, qty, unit_price, total from common formats.
    If a value isn't present, leave it as None (do not invent).
    """
    lines = [ln for ln in all_text.splitlines() if ln.strip()]
    items: List[Dict[str, Any]] = []
    curr: Dict[str, Any] = {}
    buf: List[str] = []

    def flush():
        nonlocal curr, buf
        if curr or buf:
            desc = " ".join(buf).strip() or None
            if desc:
                curr.setdefault("description", desc)
            if curr:
                items.append(curr)
        curr, buf = {}, []

    for ln in lines:
        code = re.search(_RE_ITEM, ln, flags=re.I)
        if code:
            # new item starts
            flush()
            curr = {"co_id": code.group(1)}
            buf = [ln]
            # try quick capture on same line
            mq = (
                re.search(r'\bQTY\b.*?([0-9]+)', ln, flags=re.I)
                or re.search(r'\bQty[: ]+([0-9]+)', ln, flags=re.I)
                or re.search(r'\b([0-9]+)\s*(?:Pcs|Sets|Qty)\b', ln, flags=re.I)
            )
            if mq:
                curr["qty"] = _clean_num(mq.group(1))
            mu = re.search(r'(?:Unit Price|Unit\s*Price|Rate|U\.\s*Rate)\s*'+_RE_MONEY, ln, flags=re.I)
            if mu:
                curr["unit_price_sar"] = _clean_num(mu.group(1))
            mt = re.search(r'(?:Total|Amount)\s*'+_RE_MONEY, ln, flags=re.I)
            if mt:
                curr["amount_sar"] = _clean_num(mt.group(1))
            continue

        # keep building description; also watch for qty/price/amount patterns on following lines
        if curr:
            buf.append(ln)
            if "qty" not in curr:
                mq = (
                    re.search(r'\b([0-9]+)\s*(?:Pcs|Sets|Qty)\b', ln, flags=re.I)
                    or re.search(r'\bQty[: ]+([0-9]+)', ln, flags=re.I)
                )
                if mq:
                    curr["qty"] = _clean_num(mq.group(1))
            if "unit_price_sar" not in curr:
                mu = re.search(
                    r'(?:Unit Price|Rate|U\.\s*Rate)\s*' + _RE_MONEY, ln, flags=re.I
                )
                if mu:
                    curr["unit_price_sar"] = _clean_num(mu.group(1))
            if "amount_sar" not in curr:
                mt = re.search(
                    r'(?:Total|Amount|TOTAL)\s*' + _RE_MONEY, ln, flags=re.I
                )
                if mt:
                    curr["amount_sar"] = _clean_num(mt.group(1))

    flush()
    # Compute amount if missing but qty*unit present (still not invention)
    for it in items:
        if it.get("amount_sar") is None and it.get("qty") is not None and it.get("unit_price_sar") is not None:
            it["amount_sar"] = round(it["qty"] * it["unit_price_sar"], 2)
    return items

def extract_meta(all_text: str) -> Dict[str, Any]:
    # Attempt to find vendor name and date; leave None if not found
    vendor = None
    for tag in ["Admark Creative", "Al Azal", "Modern Furnishing", "OAKTREE", "Woodwork Arts", "BURJ"]:
        if re.search(tag, all_text, flags=re.I):
            vendor = tag
            break
    date = _first([re.compile(r'\bDate[:\s]+'+_RE_DATE, re.I),
                   re.compile(r'\bDATE[:\s]+'+_RE_DATE, re.I),
                   re.compile(_RE_DATE)], all_text)
    return {"vendor_name": vendor, "doc_date": date}

def parse_procurement_pdf(data: bytes, file_url: str|None=None) -> Dict[str, Any]:
    pages = pdf_to_text_pages(data)
    joined = "\n".join(pages)
    items = greedy_line_items(joined)
    meta = extract_meta(joined)
    # Build change_orders-like rows (missing fields kept as None)
    rows: List[Dict[str, Any]] = []
    for it in items:
        rows.append({
            "project_id": None,                 # unknown (do not invent)
            "linked_cost_code": None,           # unknown (do not invent)
            "description": it.get("description"),
            "file_link": file_url,              # where the PDF is stored (if uploaded)
            "co_id": it.get("co_id"),
            "date": meta.get("doc_date"),
            "amount_sar": it.get("amount_sar"),
            "vendor_name": meta.get("vendor_name"),
            "qty": it.get("qty"),
            "unit_price_sar": it.get("unit_price_sar"),
            "source": "procurement_pdf"
        })
    return {"meta": meta, "rows": rows, "raw_preview": joined[:4000]}
