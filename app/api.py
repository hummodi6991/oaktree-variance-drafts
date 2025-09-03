from fastapi import UploadFile, File, Form

from app.main import app


# --- Single Data File endpoint ---
@app.post("/single/generate")
async def single_generate(file: UploadFile = File(...), bilingual: bool = Form(True)):
    b = await file.read()
    from app.parsers.single_file import parse_single_file

    parsed = parse_single_file(file.filename, b)

    if parsed.get("mode") == "variance":
        # Reuse existing variance -> drafts pipeline
        items = []
        for it in parsed["items"]:
            items.append(
                {
                    "project_id": it.get("label"),
                    "period": None,
                    "category": None,
                    "budget_sar": it["budget_sar"],
                    "actual_sar": it["actual_sar"],
                    "variance_sar": it["variance_sar"],
                    "variance_pct": it.get("variance_pct"),
                    "drivers": [],
                    "vendors": [],
                    "evidence_links": [],
                }
            )
        return {"mode": "variance", "items": items}

    if parsed.get("mode") == "procurement":
        from app.llm.procurement_draft import format_procurement_cards

        cards = format_procurement_cards(parsed, bilingual=bilingual)
        return {"mode": "procurement", "cards": cards}

    # Fallback summary
    return {"mode": "summary", "text": parsed.get("text", "")}

