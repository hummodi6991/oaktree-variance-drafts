from typing import List, Dict, Any


def _num(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def compute_procurement_insights(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize procurement lines for more meaningful single-file output."""
    per_vendor: Dict[str, float] = {}
    per_item_stats: Dict[str, Dict[str, float]] = {}
    top_lines: List[Dict[str, Any]] = []

    for it in items:
        vendor = (it.get("vendor_name") or "").strip() or "Unknown vendor"
        qty = _num(it.get("qty")) or 0
        unit = _num(it.get("unit_price_sar")) or 0
        amt = _num(it.get("amount_sar")) or (qty * unit)
        code = (it.get("item_code") or "").strip() or "-"

        per_vendor[vendor] = per_vendor.get(vendor, 0.0) + (amt or 0.0)

        stats = per_item_stats.setdefault(
            code, {"min_unit": None, "max_unit": None, "sum_unit": 0.0, "count": 0}
        )
        if unit:
            stats["min_unit"] = unit if stats["min_unit"] is None else min(stats["min_unit"], unit)
            stats["max_unit"] = unit if stats["max_unit"] is None else max(stats["max_unit"], unit)
            stats["sum_unit"] += unit
            stats["count"] += 1

        top_lines.append(
            {
                "vendor_name": vendor,
                "item_code": code,
                "description": it.get("description"),
                "qty": qty,
                "unit_price_sar": unit,
                "amount_sar": amt,
            }
        )

    for code, stats in per_item_stats.items():
        if stats["count"]:
            stats["avg_unit"] = stats["sum_unit"] / stats["count"]
        else:
            stats["avg_unit"] = None
        stats.pop("sum_unit", None)
        stats.pop("count", None)

    top_lines = sorted(top_lines, key=lambda r: (r.get("amount_sar") or 0.0), reverse=True)[:20]

    return {
        "totals_per_vendor": [
            {"vendor_name": v, "total_amount_sar": round(a, 2)}
            for v, a in sorted(per_vendor.items(), key=lambda x: -x[1])
        ],
        "unit_price_stats_per_item": [
            {"item_code": c, **vals} for c, vals in per_item_stats.items()
        ],
        "top_lines_by_amount": top_lines,
    }

