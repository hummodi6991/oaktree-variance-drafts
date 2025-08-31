from app.services.csv_loader import parse_csv


def test_parse_csv_maps_synonyms():
    data = b"project_id,period(YYYY-MM),date(YYYY-MM-DD),value\n1,2024-05,2024-05-06,100\n"
    rows = parse_csv(data)
    assert rows == [{"project_id": "1", "period": "2024-05", "date": "2024-05-06", "value": "100"}]


def test_parse_csv_skips_blank_rows():
    data = b"period,category\n2024-01,Alpha\n,\n"
    rows = parse_csv(data)
    assert rows == [{"period": "2024-01", "category": "Alpha"}]
