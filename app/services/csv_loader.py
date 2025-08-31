from typing import Dict, List, Iterable
import csv, io

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
