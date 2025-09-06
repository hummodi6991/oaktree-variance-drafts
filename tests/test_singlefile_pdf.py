from pathlib import Path

from app.services.singlefile import process_single_file


def test_pdf_produces_summary_and_insights():
    data = Path('samples/procurement_example.pdf').read_bytes()
    res = process_single_file('procurement_example.pdf', data)
    assert 'summary_text' in res and isinstance(res['summary_text'], str)
    assert 'analysis_text' in res and isinstance(res['analysis_text'], str)
    assert 'insights_text' in res and isinstance(res['insights_text'], str)
    assert res['summary_text'].strip()
    assert res['analysis_text'].strip()
    assert res['insights_text'].strip()
    assert 'analysis' not in res and 'insights' not in res
    assert 'items' not in res
    assert 'mode' not in res
    assert res.get('source') in ('llm', 'local')
