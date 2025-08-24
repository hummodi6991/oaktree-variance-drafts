
from typing import List, Dict, Tuple
from collections import defaultdict
from datetime import datetime
from .schemas import BudgetActualRow, ChangeOrderRow, VendorMapRow, CategoryMapRow, ConfigModel, VarianceItem

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

def filter_materiality(items: List[VarianceItem], cfg: ConfigModel) -> List[VarianceItem]:
    return [v for v in items if abs(v.variance_pct) >= cfg.materiality_pct or abs(v.variance_sar) >= cfg.materiality_amount_sar]
