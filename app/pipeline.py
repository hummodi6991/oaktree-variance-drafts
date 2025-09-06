
from typing import Callable, Dict, List, Tuple, Any
from collections import defaultdict
from datetime import datetime
from .schemas import (
    BudgetActualRow,
    ChangeOrderRow,
    VendorMapRow,
    CategoryMapRow,
    ConfigModel,
    VarianceItem,
    DraftRequest,
    DraftResponse,
)
from .gpt_client import generate_draft
from .schemas import GenerationMeta, TokenUsage

def ym_to_range(period: str) -> Tuple[datetime, datetime]:
    start = datetime.strptime(period + "-01", "%Y-%m-%d")
    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)
    return start, end

def build_category_lookup(category_map: List[CategoryMapRow]) -> Dict[str, str]:
    return {row.cost_code: row.category for row in category_map}

def group_variances(budget_actuals: List[BudgetActualRow], cat_lu: Dict[str, str]) -> List[VarianceItem]:
    agg: Dict[Tuple[str, str, str], Dict[str, float]] = defaultdict(lambda: {"budget": 0.0, "actual": 0.0})
    for row in budget_actuals:
        category = row.category or cat_lu.get(row.cost_code, "Uncategorized")
        key = (row.project_id, row.period, category)
        agg[key]["budget"] += float(row.budget_sar)
        agg[key]["actual"] += float(row.actual_sar)

    out: List[VarianceItem] = []
    for (project_id, period, category), sums in agg.items():
        budget = sums["budget"]
        actual = sums["actual"]
        variance = actual - budget
        variance_pct = 0.0 if budget == 0 else (variance / budget) * 100.0
        out.append(VarianceItem(
            project_id=project_id,
            period=period,
            category=category,
            budget_sar=budget,
            actual_sar=actual,
            variance_sar=variance,
            variance_pct=variance_pct,
        ))
    return out

def attach_drivers_and_vendors(items: List[VarianceItem], change_orders: List[ChangeOrderRow], vendor_map: List[VendorMapRow], cat_lu: Dict[str, str]) -> None:
    from collections import defaultdict
    co_by_project = defaultdict(list)
    for co in change_orders:
        co_by_project[co.project_id].append(co)

    vendors_by_project_cost = defaultdict(list)
    for vm in vendor_map:
        vendors_by_project_cost[(vm.project_id, vm.cost_code)].append(vm.vendor_name)

    for v in items:
        start, end = ym_to_range(v.period)
        drivers, evidence, vend_set = [], [], set()
        for co in co_by_project.get(v.project_id, []):
            if not co.date:
                continue
            try:
                co_date = datetime.strptime(co.date, "%Y-%m-%d")
            except Exception:
                continue
            if not co.linked_cost_code:
                continue
            co_cat = cat_lu.get(co.linked_cost_code, co.category or "")
            if co_cat == v.category and start <= co_date < end:
                d = co.description or f"Change Order {co.co_id}"
                drivers.append(f"{co.co_id}: {d}")
                if co.file_link:
                    evidence.append(co.file_link)
                for vn in vendors_by_project_cost.get((v.project_id, co.linked_cost_code), []):
                    vend_set.add(vn)
        v.drivers = drivers
        v.evidence_links = evidence
        v.vendors = sorted(vend_set)


def _summarize_change_orders(change_orders: List[ChangeOrderRow], cat_lu: Dict[str, str]) -> Dict[str, Any]:
    """Build a lightweight summary/insights object from change-order style rows."""
    from collections import defaultdict
    total = 0.0
    count = 0
    by_cat: Dict[str, float] = defaultdict(float)
    top: List[Dict[str, Any]] = []

    for co in change_orders or []:
        try:
            amt = float(co.amount_sar) if co.amount_sar is not None else None
        except Exception:
            amt = None
        if amt is None:
            continue
        count += 1
        total += amt
        cat = co.category or cat_lu.get(co.linked_cost_code or "", "Uncategorized")
        by_cat[cat] += amt
        top.append({
            "project_id": co.project_id or "Unknown",
            "date": co.date,
            "category": cat,
            "co_id": getattr(co, "co_id", None),
            "description": co.description,
            "amount_sar": amt,
            "file_link": co.file_link,
            "vendor_name": getattr(co, "vendor_name", None),
        })

    top = sorted(top, key=lambda r: r.get("amount_sar") or 0.0, reverse=True)[:20]
    top_cat = max(by_cat.items(), key=lambda x: x[1]) if by_cat else (None, 0.0)
    highlights = [
        f"{count} change order(s) totaling {round(total, 2):,} SAR.",
    ]
    if top_cat[0]:
        highlights.append(
            f"Highest category: {top_cat[0]} ({round(top_cat[1], 2):,} SAR)."
        )
    return {
        "kind": "summary",
        "message": "No budget/actuals detected — showing change-order summary.",
        "insights": {
            "total_change_orders": count,
            "total_amount_sar": round(total, 2),
            "totals_by_category": [
                {"category": k, "total_amount_sar": round(v, 2)}
                for k, v in sorted(by_cat.items(), key=lambda x: -x[1])
            ],
            "top_change_orders_by_amount": top,
            "highlights": highlights,
        },
    }

def _choose_amount_key(sample: Dict[str, Any]) -> str | None:
    """Pick an amount-like column from arbitrary tabular rows (case-insensitive)."""
    if not sample:
        return None
    keys = [k for k in sample.keys()]
    candidates = [
        "amount_sar", "amount", "value", "price", "cost", "total", "total_sar", "net_amount",
    ]
    lower = {k.lower(): k for k in keys}
    for c in candidates:
        if c in lower:
            return lower[c]
    for k in keys:
        v = sample[k]
        if isinstance(v, (int, float)):
            return k
    return None

def _summarize_generic_rows(rows: List[Dict[str, Any]], label: str = "rows") -> Dict[str, Any]:
    """Generic summarizer for arbitrary uploaded tables without budget/actuals."""
    from collections import defaultdict
    rows = rows or []
    if not rows:
        return {
            "kind": "summary",
            "message": "No budget/actuals detected — file contained no tabular rows to summarize.",
            "insights": {},
        }
    amount_key = _choose_amount_key(rows[0])
    if not amount_key:
        return {
            "kind": "summary",
            "message": "No budget/actuals detected — could not identify an amount column to summarize.",
            "insights": {
                "row_count": len(rows),
                "sample_keys": list(rows[0].keys()),
            },
        }
    group_keys: List[str] = []
    preferred = ["category", "cost_code", "linked_cost_code", "vendor", "vendor_name", "project_id"]
    lower = {k.lower(): k for k in rows[0].keys()}
    for p in preferred:
        if p in lower:
            group_keys.append(lower[p])
    total = 0.0
    by_group: Dict[str, float] = defaultdict(float)
    top: List[Dict[str, Any]] = []
    for r in rows:
        try:
            amt = float(r.get(amount_key)) if r.get(amount_key) is not None else None
        except Exception:
            amt = None
        if amt is None:
            continue
        total += amt
        if group_keys:
            gvals = [str(r.get(k) or "Uncategorized") for k in group_keys]
            glabel = " / ".join(gvals)
            by_group[glabel] += amt
        short = {k: r.get(k) for k in [amount_key] + group_keys if k in r}
        top.append(short)
    top = sorted(top, key=lambda d: d.get(amount_key) or 0.0, reverse=True)[:20]
    totals_by_group = [
        {"group": k, "total_amount": round(v, 2)} for k, v in sorted(by_group.items(), key=lambda x: -x[1])
    ]
    top_group = max(totals_by_group, key=lambda x: x["total_amount"], default=None)
    highlights = [
        f"{len(rows)} {label} row(s) totaling {round(total, 2):,} in {amount_key}.",
    ]
    if top_group:
        highlights.append(
            f"Top group: {top_group['group']} ({top_group['total_amount']:,})."
        )
    return {
        "kind": "summary",
        "message": "No budget/actuals detected — showing generic summary.",
        "insights": {
            "row_count": len(rows),
            "amount_column": amount_key,
            "total_amount": round(total, 2),
            "totals_by_group": totals_by_group,
            "top_rows_by_amount": top,
            "label": label,
            "highlights": highlights,
        },
    }

def filter_materiality(items: List[VarianceItem], cfg: ConfigModel) -> List[VarianceItem]:
    return [v for v in items if abs(v.variance_pct) >= cfg.materiality_pct or abs(v.variance_sar) >= cfg.materiality_amount_sar]


def _noop_progress(pct: int, msg: str = "") -> None:
    return None


def generate_drafts(
    req: DraftRequest,
    progress_cb: Callable[[int, str], None] = _noop_progress,
    *,
    force_local: bool = False,
) -> Tuple[Any, GenerationMeta]:
    """High-level helper to build drafts from CSV-derived models."""
    progress_cb(10, "Loading & validating input")
    cat_lu = build_category_lookup(req.category_map)

    progress_cb(25, "Computing variances")
    items = group_variances(req.budget_actuals, cat_lu)
    has_pairs = any(v.budget_sar and v.actual_sar for v in items)
    if items and has_pairs:
        attach_drivers_and_vendors(items, req.change_orders, req.vendor_map, cat_lu)
    else:
        # No budget/actual pairs: return summary+insights
        rows = [
            r.model_dump() if hasattr(r, "model_dump") else (
                r.dict() if hasattr(r, "dict") else dict(r)
            )
            for r in (getattr(req, "budget_actuals", []) or [])
        ]
        if rows:
            progress_cb(40, "Summarizing budget/actuals")
            summary = _summarize_generic_rows(rows, label="budget_actuals")
            summary["message"] = "No budget/actual pairs detected — showing summary."
            return summary, GenerationMeta(llm_used=False, forced_local=force_local)
        if req.change_orders:
            progress_cb(40, "Summarizing change orders")
            return _summarize_change_orders(req.change_orders, cat_lu), GenerationMeta(
                llm_used=False, forced_local=force_local
            )
        if getattr(req, "raw_rows", None):
            progress_cb(40, "Summarizing uploaded rows")
            return _summarize_generic_rows(req.raw_rows, label="single_file"), GenerationMeta(
                llm_used=False, forced_local=force_local
            )
        if getattr(req, "vendor_map", None):
            progress_cb(40, "Summarizing vendor map")
            rows = [
                v.model_dump() if hasattr(v, "model_dump") else (
                    v.dict() if hasattr(v, "dict") else dict(v)
                )
                for v in req.vendor_map
            ]
            return _summarize_generic_rows(rows, label="vendor_map"), GenerationMeta(
                llm_used=False, forced_local=force_local
            )
        if getattr(req, "category_map", None):
            progress_cb(40, "Summarizing category map")
            rows = [
                v.model_dump() if hasattr(v, "model_dump") else (
                    v.dict() if hasattr(v, "dict") else dict(v)
                )
                for v in req.category_map
            ]
            return _summarize_generic_rows(rows, label="category_map"), GenerationMeta(
                llm_used=False, forced_local=force_local
            )
        return (
            {
                "kind": "summary",
                "message": "No budget/actuals detected and no tabular data available to summarize.",
                "insights": {},
            },
            GenerationMeta(llm_used=False, forced_local=force_local),
        )

    progress_cb(55, "Preparing EN prompt")
    material = filter_materiality(items, req.config)
    if not material:
        rows = [
            r.model_dump() if hasattr(r, "model_dump") else (
                r.dict() if hasattr(r, "dict") else dict(r)
            )
            for r in req.budget_actuals
        ]
        progress_cb(60, "Summarizing budget/actuals")
        summary = _summarize_generic_rows(rows, label="budget_actuals")
        summary["message"] = "No variances met materiality — showing summary."
        return summary, GenerationMeta(llm_used=False, forced_local=force_local)
    out: List[DraftResponse] = []
    meta = GenerationMeta(llm_used=False, forced_local=force_local)
    usage_totals = TokenUsage()
    for v in material:
        progress_cb(75, "Calling model (EN)")
        en, ar, m = generate_draft(v, req.config, local_only=force_local)
        progress_cb(85, "Calling model (AR)")
        out.append(DraftResponse(variance=v, draft_en=en, draft_ar=ar or None))
        if m.llm_used:
            meta.llm_used = True
            meta.provider = m.provider
            meta.model = m.model
            if m.token_usage:
                usage_totals.prompt_tokens = (usage_totals.prompt_tokens or 0) + (
                    m.token_usage.prompt_tokens or 0
                )
                usage_totals.completion_tokens = (usage_totals.completion_tokens or 0) + (
                    m.token_usage.completion_tokens or 0
                )
                usage_totals.total_tokens = (usage_totals.total_tokens or 0) + (
                    m.token_usage.total_tokens or 0
                )

    if meta.llm_used:
        meta.token_usage = usage_totals

    progress_cb(95, "Finalizing result")
    progress_cb(100, "Done")
    return out, meta
