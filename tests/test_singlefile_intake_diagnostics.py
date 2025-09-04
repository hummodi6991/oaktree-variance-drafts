import io
import pandas as pd

from app.parsers.single_file_intake import parse_single_file


def _csv_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_csv(bio, index=False)
    return bio.getvalue()


def test_parse_single_file_returns_diagnostics():
    df = pd.DataFrame({"budget": [100], "actual": [120]})
    b = _csv_bytes(df)
    resp = parse_single_file("simple.csv", b)
    assert "diagnostics" in resp
    diag = resp["diagnostics"]
    assert diag.get("correlation_id")
    assert isinstance(diag.get("events"), list)
