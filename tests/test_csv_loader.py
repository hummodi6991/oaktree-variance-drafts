from io import BytesIO
import pandas as pd

from app.services.csv_loader import parse_csv, parse_tabular


def test_parse_csv_maps_synonyms():
    data = b"project_id,period(YYYY-MM),date(YYYY-MM-DD),value\n1,2024-05,2024-05-06,100\n"
    rows = parse_csv(data)
    assert rows == [{"project_id": "1", "period": "2024-05", "date": "2024-05-06", "value": "100"}]


def test_parse_csv_skips_blank_rows():
    data = b"period,category\n2024-01,Alpha\n,\n"
    rows = parse_csv(data)
    assert rows == [{"period": "2024-01", "category": "Alpha"}]


def test_parse_csv_semicolon_delimiter():
    data = b"project_id;period(YYYY-MM);date(YYYY-MM-DD);value\n1;2024-05;2024-05-06;100\n"
    rows = parse_csv(data)
    assert rows == [{"project_id": "1", "period": "2024-05", "date": "2024-05-06", "value": "100"}]


def test_parse_tabular_excel():
    df = pd.DataFrame(
        [{"project_id": 1, "period(YYYY-MM)": "2024-05", "value": 100}]
    )
    buf = BytesIO()
    df.to_excel(buf, index=False)
    rows = parse_tabular(buf.getvalue(), "test.xlsx")
    assert rows == [{"project_id": 1, "period": "2024-05", "value": 100}]


def test_parse_csv_excel_fallback():
    """Binary Excel uploads should still be parsed by parse_csv."""
    df = pd.DataFrame(
        [{"project_id": 2, "period(YYYY-MM)": "2024-06", "value": 200}]
    )
    buf = BytesIO()
    df.to_excel(buf, index=False)
    rows = parse_csv(buf.getvalue())
    assert rows == [{"project_id": 2, "period": "2024-06", "value": 200}]


def test_parse_tabular_semicolon_csv():
    data = b"project_id;period(YYYY-MM);value\n1;2024-07;300\n"
    rows = parse_tabular(data, "test.csv")
    assert rows == [{"project_id": 1, "period": "2024-07", "value": 300}]
