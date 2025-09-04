import io
import pandas as pd
from app.services.singlefile import process_single_file


def _xlsx_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xl:
        df.to_excel(xl, index=False)
    return bio.getvalue()


def test_doors_quotes_like_excel_parses_to_summary():
    # Simulate a sheet with a preamble row, then real headers on row 3
    data = [
        ["Vendor:", "AL AZAL", "", "", ""],
        ["", "", "", "", ""],
        ["Item No", "Description of Works", "Qty", "Unit Rate (SAR)", "Total Price (SAR)"],
        ["D01", "Fire-rated door 90min", 2, 1500, 3000],
        ["D02", "Acoustic door", 1, 2200, 2200],
    ]
    df = pd.DataFrame(data, columns=[f"C{i}" for i in range(1,6)])
    b = _xlsx_bytes(df)
    resp = process_single_file("doors_quotes_complete.xlsx", b)
    assert resp["mode"] == "summary"
    items = resp["items"]
    assert len(items) >= 2
    first = items[0]
    assert first.get("item_code") in ("D01", "D1", "D01")
    assert first.get("unit_price_sar") == 1500
    assert first.get("amount_sar") == 3000
    assert any((it.get("vendor") == "AL AZAL") for it in items)

