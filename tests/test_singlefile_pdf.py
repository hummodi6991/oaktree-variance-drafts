from pathlib import Path
from app.services.singlefile import process_single_file


def test_pdf_produces_summary_and_insights():
    data = Path('samples/procurement_example.pdf').read_bytes()
    res = process_single_file('procurement_example.pdf', data)
    assert set(res.keys()) == {"summary", "analysis", "insights"}
    assert all(isinstance(res[k], str) for k in res)
