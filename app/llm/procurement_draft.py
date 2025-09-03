from typing import Dict, Any, List


def format_procurement_cards(payload: Dict[str, Any], bilingual: bool) -> List[Dict[str, Any]]:
    cards = []
    vendor = payload.get("vendor_name")
    doc_date = payload.get("doc_date")
    for i, row in enumerate(payload.get("items", []), start=1):
        title = f"{row.get('item_code') or 'Item'} — SAR {row.get('amount_sar') or row.get('unit_price_sar')}"
        en = []
        if vendor:
            en.append(f"Vendor: {vendor}")
        if doc_date:
            en.append(f"Document date: {doc_date}")
        if row.get("qty") is not None:
            en.append(f"Qty: {row['qty']}")
        if row.get("unit_price_sar") is not None:
            en.append(f"Unit price: SAR {row['unit_price_sar']}")
        if row.get("amount_sar") is not None:
            en.append(f"Line amount: SAR {row['amount_sar']}")
        ar = []
        if bilingual:
            if vendor:
                ar.append(f"المورّد: {vendor}")
            if doc_date:
                ar.append(f"تاريخ المستند: {doc_date}")
            if row.get("qty") is not None:
                ar.append(f"الكمية: {row['qty']}")
            if row.get("unit_price_sar") is not None:
                ar.append(f"سعر الوحدة: {row['unit_price_sar']} ر.س")
            if row.get("amount_sar") is not None:
                ar.append(f"قيمة البند: {row['amount_sar']} ر.س")
        cards.append(
            {
                "title": title,
                "body_en": " · ".join(en) or "No details found in the document.",
                "body_ar": " · ".join(ar) if bilingual else None,
                "source": "Uploaded procurement file",
            }
        )
    if not cards:
        cards = [
            {
                "title": "Summary",
                "body_en": "No structured line items were found.",
                "body_ar": None,
                "source": "Uploaded procurement file",
            }
        ]
    return cards

