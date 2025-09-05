import pathlib
import asyncio

from app.parsers.single_file import analyze_single_file


def test_analyze_single_file_discards_cards():
    pdf_path = pathlib.Path('samples/procurement_example.pdf')
    data = pdf_path.read_bytes()
    res = asyncio.run(analyze_single_file(data, pdf_path.name))
    assert "summary" in res and "analysis" in res and "insights" in res
    assert "items" not in res.get("summary", {})
