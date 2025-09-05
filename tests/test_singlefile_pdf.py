from pathlib import Path
from app.services.singlefile import process_single_file


def test_pdf_produces_summary_and_insights():
    data = Path('samples/procurement_example.pdf').read_bytes()
    res = process_single_file('procurement_example.pdf', data)
    assert 'summary' in res and isinstance(res['summary'], dict)
    assert isinstance(res.get('analysis'), dict)
    assert isinstance(res.get('insights'), dict)
    assert 'items' not in res
    assert isinstance(res.get('summary_text'), str)
