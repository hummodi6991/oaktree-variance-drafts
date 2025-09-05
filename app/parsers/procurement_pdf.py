from typing import List, Dict, Any
import io
import time
from pdfminer.high_level import extract_text
import pdfplumber
import re
import io
from typing import Dict, Any, List

# Fallback: detect unlabeled "qty unit_price total" rows
ROW_TRIPLET = re.compile(
    r"\b(\d{1,3})\s+([0-9]{1,3}(?:[, ]\d{3})*(?:\.\d+)?)\s+([0-9]{1,3}(?:[, ]\d{3})*(?:\.\d+)?)\b"
)

AMOUNT = r"([0-9]{1,3}(?:,\d{3})*(?:\.\d{2}))"
PAIR_AMOUNTS = re.compile(fr"{AMOUNT}\s+{AMOUNT}")

RE_DATE = re.compile(r'\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|(?:\d{4}-\d{2}-\d{2}))\b')
RE_ITEM = re.compile(r'\b(D0?\d|Item\s*No\.?\s*:?\s*\d+)\b', re.I)
RE_QTY = re.compile(r'\b(?:QTY|Quantity)\s*[:=]?\s*(\d+(?:\.\d+)?)', re.I)
RE_PRICE = re.compile(r'\b(?:Unit\s*Price|U\.?\s*Rate|Price)\s*(?:in\s*SAR)?\s*[:=]?\s*(\d{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_TOTAL = re.compile(r'\b(?:TOTAL|Amount|Grand\s*Total)\s*(?:in\s*SAR)?\s*[:=]?\s*(\d{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_VENDOR = re.compile(r'\b(?:Admark Creative|AL AZAL|Al Azal|Modern Furnishing|Woodwork Arts|Burj|OAKTREE|Oaktree|Alam)\b.*', re.I)

# Totals / meta cues
RE_SUBTOTAL = re.compile(r'\bsub\s*total\s*[:=]?\s*(\d{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_VAT_AMT = re.compile(r'\bvat[^\d%]*([0-9]{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_VAT_RATE = re.compile(r'\bvat\s*(\d{1,2}(?:\.\d+)?)[%]', re.I)
RE_GRAND_TOTAL = re.compile(r'\bgrand\s*total\s*[:=]?\s*(\d{1,3}(?:[, ]\d{3})*(?:\.\d+)?)', re.I)
RE_VALIDITY = re.compile(r'validity\s*(?:days)?\s*[:=]?\s*(\d+)', re.I)
RE_PAYMENT = re.compile(r'payment\s*terms?\s*[:\-]?\s*(.+)', re.I)

PR_CUES = ["item code", "qty/unit", "requested", "approved"]
QUOTE_CUES = ["quotation", "quote #", "quote no", "grand total", "vat 15", "validity"]
COMP_CUES = ["best price", "comparison"]

UNIT_MAP = {
    "sets": "unit",
    "set": "unit",
    "pcs": "unit",
    "pcs.": "unit",
    "pieces": "unit",
    "nos": "unit",
    "unit": "unit",
}


def _num(s: str) -> float | None:
    if not s:
        return None
    s = s.replace(',', '').replace('SAR', '').strip()
    try:
        return float(s)
    except:  # noqa: E722
        return None


def _extract_text_safe(pdf_bytes: bytes) -> str:
    """Try pdfminer, fall back to pdfplumber on failure."""
    try:
        return extract_text(io.BytesIO(pdf_bytes)) or ""
    except Exception:
        try:
            buf = io.BytesIO(pdf_bytes)
            all_text: list[str] = []
            with pdfplumber.open(buf) as pdf:
                for page in pdf.pages:
                    all_text.append(page.extract_text() or "")
            return "\n".join(all_text)
        except Exception:
            return ""


def _classify_page(txt: str) -> str:
    lt = txt.lower()
    if any(c in lt for c in QUOTE_CUES):
        return "quote"
    if any(c in lt for c in PR_CUES):
        return "pr_boq"
    if any(c in lt for c in COMP_CUES) or lt.count("vendor") > 1:
        return "comparison"
    return "unknown"


def _norm_unit(txt: str | None) -> str | None:
    if not txt:
        return None
    u = txt.strip().lower()
    return UNIT_MAP.get(u, u)


def parse_procurement_pdf(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Extract line items without inventing anything.
    Returns {"items":[{item_code, description, unit, qty, unit_price_sar, amount_sar, vendor_name, doc_date}], "meta":{...}}
    Missing fields stay None.
    """
    started = time.time()
    text = _extract_text_safe(pdf_bytes)

    page_types: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                pt = page.extract_text() or ""
                page_types.append(_classify_page(pt))
    except Exception:
        pass

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

        unit = None
        um = re.search(r'\b(SETS?|PCS|PIECES|NOS|UNIT|M2|M3)\b', ch, re.I)
        if um:
            unit = _norm_unit(um.group(0))

        items.append({
            "item_code": item_code,
            "description": description,
            "unit": unit,
            "qty": qty,
            "unit_price_sar": unit_price,
            "amount_sar": amount,
            "vendor_name": vendor,
            "doc_date": date,
            "source": "uploaded_file",
        })

    # Fallback: unlabeled "qty price amount" rows, e.g. "SETS 18 2,000.00 36,000.00"
    if not items and text:
        for q, up, amt in ROW_TRIPLET.findall(text):
            items.append({
                "item_code": None,
                "description": None,
                "qty": _num(q),
                "unit_price_sar": _num(up),
                "amount_sar": _num(amt),
                "vendor_name": vendor,
                "doc_date": date,
                "source": "uploaded_file",
            })

    # Fallback 2: two adjacent amounts (unit + total); infer qty = total / unit
    if not items and text:
        for u, t in PAIR_AMOUNTS.findall(text):
            unit = _num(u)
            total = _num(t)
            if unit and total and unit > 0:
                qty = round(total / unit)
                if 0 < qty < 1000 and abs(total - qty * unit) <= max(1.0, 0.02 * total):
                    items.append({
                        "item_code": None,
                        "description": None,
                        "qty": qty,
                        "unit_price_sar": unit,
                        "amount_sar": total,
                        "vendor_name": vendor,
                        "doc_date": date,
                        "source": "uploaded_file",
                    })

    # Extra guard — if extraction took too long and we still have nothing, return fast.
    if time.time() - started > 20 and not items:
        return {"items": [], "meta": {"vendor_name": vendor, "doc_date": date}}

    # Totals & meta extraction from whole text
    subtotal = _num(RE_SUBTOTAL.search(text).group(1)) if RE_SUBTOTAL.search(text) else None
    vat_amt = _num(RE_VAT_AMT.search(text).group(1)) if RE_VAT_AMT.search(text) else None
    vat_rate = _num(RE_VAT_RATE.search(text).group(1)) if RE_VAT_RATE.search(text) else None
    grand_total = _num(RE_GRAND_TOTAL.search(text).group(1)) if RE_GRAND_TOTAL.search(text) else None
    validity = RE_VALIDITY.search(text)
    validity_days = int(validity.group(1)) if validity else None
    payment_m = RE_PAYMENT.search(text)
    payment_terms = payment_m.group(1).strip() if payment_m else None

    lt = text.lower()
    delivery_included = None
    if "delivery" in lt:
        if re.search(r"delivery[^\n]*not\s+included", lt):
            delivery_included = False
        elif re.search(r"delivery[^\n]*included", lt):
            delivery_included = True
    hardware_included = None
    if "hardware" in lt:
        if re.search(r"hardware[^\n]*not\s+included", lt):
            hardware_included = False
        elif re.search(r"hardware[^\n]*included", lt):
            hardware_included = True

    meta = {
        "vendor_name": vendor,
        "doc_date": date,
        "page_types": page_types,
        "doc_type": max(page_types, key=page_types.count) if page_types else None,
        "subtotal_amount_sar": subtotal,
        "vat_amount_sar": vat_amt,
        "vat_pct": vat_rate,
        "grand_total_sar": grand_total,
        "validity_days": validity_days,
        "payment_terms": payment_terms,
        "delivery_included": delivery_included,
        "hardware_included": hardware_included,
    }

    if subtotal and vat_rate and not vat_amt:
        meta["vat_amount_recalc_sar"] = round(subtotal * (vat_rate / 100.0), 2)
    return {"items": items, "meta": meta}
