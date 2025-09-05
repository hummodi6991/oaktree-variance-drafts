from fastapi import UploadFile, File, Form

from app.main import app


# --- Single Data File endpoint ---
@app.post("/single/generate")
async def single_generate(file: UploadFile = File(...), bilingual: bool = Form(True)):
    b = await file.read()
    from app.parsers.single_file_intake import parse_single_file

    parsed = parse_single_file(file.filename, b)

    report_type = parsed.get("report_type")
    mode = parsed.get("mode")

    if "variance_items" in parsed:
        source_items = parsed.get("variance_items", [])
        items = []
        for it in source_items:
            items.append(
                {
                    "project_id": it.get("label"),
                    "period": None,
                    "category": None,
                    "budget_sar": it.get("budget_sar"),
                    "actual_sar": it.get("actual_sar"),
                    "variance_sar": it.get("variance_sar"),
                    "variance_pct": it.get("variance_pct"),
                    "drivers": [],
                    "vendors": [],
                    "evidence_links": [],
                }
            )
        return {"mode": "variance", "items": items}

    if mode == "variance" or report_type == "variance_insights":
        items = []
        for it in parsed.get("items", []):
            items.append(
                {
                    "project_id": it.get("label"),
                    "period": None,
                    "category": None,
                    "budget_sar": it.get("budget_sar"),
                    "actual_sar": it.get("actual_sar"),
                    "variance_sar": it.get("variance_sar"),
                    "variance_pct": it.get("variance_pct"),
                    "drivers": [],
                    "vendors": [],
                    "evidence_links": [],
                }
            )
        return {"mode": "variance", "items": items}

    if "procurement_summary" in parsed:
        from app.llm.procurement_draft import format_procurement_cards

        ps = parsed.get("procurement_summary", {})
        meta = ps.get("meta", {}) if isinstance(ps, dict) else {}
        payload = {
            "items": ps.get("items", []),
            "vendor_name": meta.get("vendor_name"),
            "doc_date": meta.get("doc_date"),
        }
        cards = format_procurement_cards(payload, bilingual=bilingual)
        return {"mode": "procurement", "cards": cards}

    if mode == "procurement" or report_type == "procurement_summary":
        from app.llm.procurement_draft import format_procurement_cards

        payload = {
            "items": parsed.get("items", []),
            "vendor_name": parsed.get("vendor_name"),
            "doc_date": parsed.get("doc_date"),
        }
        cards = format_procurement_cards(payload, bilingual=bilingual)
        return {"mode": "procurement", "cards": cards}

    # Fallback summary
    return {"mode": "summary", "text": parsed.get("text", "")}

