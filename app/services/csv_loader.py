from typing import Dict, Iterable, List
import csv
import io
import pandas as pd

# Map alternate headers seen in uploads to canonical names the API expects
HEADER_SYNONYMS: Dict[str, Iterable[str]] = {
    "period": ["period", "period(YYYY-MM)"],
    "date": ["date", "date(YYYY-MM-DD)"],
}

def _canonicalize_headers(headers: List[str]) -> List[str]:
    canon = []
    for h in headers:
        h_clean = h.strip()
        mapped = None
        for target, alts in HEADER_SYNONYMS.items():
            if h_clean in alts:
                mapped = target
                break
        canon.append(mapped or h_clean)
    return canon

def parse_csv(upload_bytes: bytes) -> List[Dict]:
    text = upload_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return []
    headers = _canonicalize_headers(rows[0])
    out = []
    for r in rows[1:]:
        if not any(x.strip() for x in r):
            continue
        out.append({headers[i]: r[i].strip() if i < len(r) else "" for i in range(len(headers))})
    return out


def parse_tabular(upload_bytes: bytes, filename: str) -> List[Dict]:
    """Parse CSV or Excel upload bytes into list of row dicts.

    This helper mirrors :func:`parse_csv` but also handles ``.xls``/``.xlsx``
    files using :mod:`pandas`.
    """
    name = (filename or "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(upload_bytes))
    elif name.endswith(".xls") or name.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(upload_bytes))
    else:
        raise ValueError(f"Unsupported file type for {filename}")

    headers = _canonicalize_headers([str(c) for c in df.columns])
    df.columns = headers
    out: List[Dict] = []
    for row in df.fillna("").to_dict(orient="records"):
        if not any(str(v).strip() for v in row.values()):
            continue
        out.append({k: (str(v).strip() if isinstance(v, str) else v) for k, v in row.items()})
    return out
