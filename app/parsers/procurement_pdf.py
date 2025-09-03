from typing import List, Dict, Any
import io
from pdfminer.high_level import extract_text
import re

RE_DATE = re.compile(r'\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|(?:\d{4}-\d{2}-\d{2}))\b')
RE_ITEM = re.compile(r'\b(D0?\d|Item\s*No\.?\s*:?\s*\d+)\b', re.I)
RE_QTY = re.compile(r'\b(?:QTY|Quantity)\s*[:=]?\s*(\d+(?:\.\d+)?)', re.I)
RE_PRICE = re.compile(r'\b(?:Unit\s*Price|U\.?\s*Rate|Price)\s*(?:in\s*SAR)?\s*[:=]?\s*(\d{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_TOTAL = re.compile(r'\b(?:TOTAL|Amount|Grand\s*Total)\s*(?:in\s*SAR)?\s*[:=]?\s*(\d{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_VENDOR = re.compile(r'\b(?:Admark Creative|AL AZAL|Al Azal|Modern Furnishing|Woodwork Arts|Burj|OAKTREE|Oaktree|Alam)\b.*', re.I)


def _num(s: str) -> float | None:
    if not s:
        return None
    s = s.replace(',', '').replace('SAR', '').strip()
    try:
        return float(s)
    except:  # noqa: E722
        return None


def parse_procurement_pdf(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Extract line items without inventing anything.
    Returns {"items":[{item_code, description, qty, unit_price_sar, amount_sar, vendor_name, doc_date}], "meta":{...}}
    Missing fields stay None.
    """
    text = extract_text(io.BytesIO(pdf_bytes)) or ""
    # Doc date / vendor (best-effort, no invention)
    date = None
    m = RE_DATE.search(text)
    if m:
        date = m.group(0)

    vendor = None
    vm = RE_VENDOR.search(text)
    if vm:
        vendor = vm.group(0).strip()

    # Split into blocks around item markers to keep descriptions intact
    chunks = re.split(r'(?=(?:^|\n).{0,10}(?:D0?\d\b|Item\s*No\.?\s*:?\s*\d+))', text, flags=re.I)
    items: List[Dict[str, Any]] = []

    for ch in chunks:
        if not RE_ITEM.search(ch):
            continue
        code_m = RE_ITEM.search(ch)
        item_code = code_m.group(1).strip() if code_m else None

        # description: take the paragraph following the code line
        # keep only what exists in the PDF (truncate excessive whitespace)
        desc_lines = [ln.strip() for ln in ch.splitlines() if ln.strip()]
        description = " ".join(desc_lines[:20])[:2000] if desc_lines else None

        qty = None
        q = RE_QTY.search(ch)
        if q:
            qty = _num(q.group(1))

        unit_price = None
        up = RE_PRICE.search(ch)
        if up:
            unit_price = _num(up.group(1))

        amount = None
        tm = RE_TOTAL.search(ch)
        if tm:
            amount = _num(tm.group(1))
        elif qty and unit_price:
            amount = round(qty * unit_price, 2)

        items.append({
            "item_code": item_code,
            "description": description,
            "qty": qty,
            "unit_price_sar": unit_price,
            "amount_sar": amount,
            "vendor_name": vendor,
            "doc_date": date,
            "source": "uploaded_file",
        })

    return {"items": items, "meta": {"vendor_name": vendor, "doc_date": date}}
