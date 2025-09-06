import pathlib
import asyncio

from app.parsers.single_file import analyze_single_file


def test_analyze_single_file_discards_cards():
    pdf_path = pathlib.Path('samples/procurement_example.pdf')
    data = pdf_path.read_bytes()
    res = asyncio.run(analyze_single_file(data, pdf_path.name))
    assert isinstance(res.get("summary_text"), str)
    assert res.get("summary_text").strip()
    assert isinstance(res.get("analysis_text"), str)
    assert res.get("analysis_text").strip()
    assert isinstance(res.get("insights_text"), str)
    assert res.get("insights_text").strip()
    assert "summary" not in res and "analysis" not in res and "insights" not in res
