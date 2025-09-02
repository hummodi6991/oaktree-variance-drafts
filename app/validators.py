"""Validation utilities for uploaded data."""
from typing import Any, Dict, List


def validate_change_orders(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Tolerant validation: only warn when key fields are missing.
    We never invent fields; missing remains ``None``.
    """
    problems = []
    for i, r in enumerate(rows or []):
        for k in ["co_id", "date", "amount_sar"]:
            if r.get(k) in (None, ""):
                problems.append({"row": i, "field": k, "msg": "missing (accepted)"})
    return {"ok": True, "warnings": problems}
