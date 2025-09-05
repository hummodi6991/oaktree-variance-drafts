import io
from typing import Any

import pandas as pd

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pdfplumber = None  # type: ignore

try:
    import docx  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    docx = None  # type: ignore


def file_bytes_to_text(filename: str, data: bytes) -> str:
    """Best-effort conversion of common document types to plain text."""
    name = (filename or "").lower()
    try:
        if name.endswith(".pdf") and pdfplumber is not None:
            with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[attr-defined]
                return "\n".join([p.extract_text() or "" for p in pdf.pages])
        if name.endswith((".xlsx", ".xls")):
            xl = pd.ExcelFile(io.BytesIO(data))
            parts = []
            for sn in xl.sheet_names:
                try:
                    df = xl.parse(sn)
                    parts.append(df.to_csv(index=False))
                except Exception:
                    continue
            return "\n".join(parts)
        if name.endswith(".csv"):
            return data.decode("utf-8", errors="ignore")
        if name.endswith((".docx", ".doc")) and docx is not None:
            d = docx.Document(io.BytesIO(data))  # type: ignore[attr-defined]
            return "\n".join(p.text for p in d.paragraphs)
        if name.endswith((".txt", ".md")):
            return data.decode("utf-8", errors="ignore")
    except Exception:
        pass
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""
