import pathlib
import asyncio

from app.parsers.single_file import analyze_single_file


def test_analyze_single_file_discards_cards():
    pdf_path = pathlib.Path('samples/procurement_example.pdf')
    data = pdf_path.read_bytes()
    res, meta = asyncio.run(analyze_single_file(data, pdf_path.name))
    assert res.get("report_type") == "summary"
    assert "procurement_summary" in res and isinstance(res["procurement_summary"], dict)
    assert "analysis" in res and isinstance(res["analysis"], dict)
    assert isinstance(meta.llm_used, bool)
