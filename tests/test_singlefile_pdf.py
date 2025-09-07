from pathlib import Path

from app.services.singlefile import process_single_file


def test_pdf_produces_summary_and_insights():
    data = Path('samples/procurement_example.pdf').read_bytes()
    res = process_single_file('procurement_example.pdf', data)
    assert res['report_type'] == 'summary'
    assert 'procurement_summary' in res and isinstance(res['procurement_summary'], dict)
    assert 'analysis' in res and isinstance(res['analysis'], dict)
    assert 'insights' in res and isinstance(res['insights'], dict)
    assert 'variance_items' not in res
    meta = res.get('_meta', {})
    assert isinstance(meta.get('llm_used'), bool)
