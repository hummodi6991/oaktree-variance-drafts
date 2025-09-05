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


def test_quote_classification(monkeypatch):
    text = (
        "Quotation #123\n"
        "Item Code D01\nQty 1\nUnit Price 100\nTotal 100\n"
        "Subtotal 100\nVAT 15% 15\nGrand Total 115\n"
        "Validity 30 days\nPayment terms: 50% advance\n"
        "Delivery included\nHardware not included"
    )

    class DummyPage:
        def __init__(self, t):
            self.t = t

        def extract_text(self):
            return self.t

    class DummyPDF:
        def __init__(self, t):
            self.pages = [DummyPage(t)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr("app.parsers.procurement_pdf.extract_text", lambda *a, **k: text)
    monkeypatch.setattr("app.parsers.procurement_pdf.pdfplumber.open", lambda *a, **k: DummyPDF(text))

    result = parse_procurement_pdf(b"%PDF-1.4")
    meta = result["meta"]
    assert meta["doc_type"] == "quote"
    assert meta["grand_total_sar"] == 115.0
    assert meta["vat_pct"] == 15.0
    assert meta["validity_days"] == 30
    assert meta["payment_terms"] == "50% advance"
    assert meta["delivery_included"] is True
    assert meta["hardware_included"] is False
