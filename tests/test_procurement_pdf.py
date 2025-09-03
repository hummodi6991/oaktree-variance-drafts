from app.parsers.procurement_pdf import parse_procurement_pdf

def test_triplet_fallback(monkeypatch):
    text = "1 10 10\n2 5,000 10,000"
    monkeypatch.setattr("app.parsers.procurement_pdf.extract_text", lambda *a, **k: text)
    result = parse_procurement_pdf(b"%PDF-1.4")
    items = result["items"]
    assert len(items) == 2
    assert items[0]["qty"] == 1
    assert items[0]["unit_price_sar"] == 10.0
    assert items[0]["amount_sar"] == 10.0
    assert items[1]["qty"] == 2
    assert items[1]["unit_price_sar"] == 5000.0
    assert items[1]["amount_sar"] == 10000.0
