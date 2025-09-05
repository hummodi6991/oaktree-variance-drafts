import csv
from pathlib import Path

from app.pipeline import (
    build_category_lookup,
    group_variances,
    attach_drivers_and_vendors,
    filter_materiality,
    generate_drafts,
)
from app.schemas import (
    BudgetActualRow,
    ChangeOrderRow,
    VendorMapRow,
    CategoryMapRow,
    ConfigModel,
    DraftRequest,
)


def _load_csv(path: Path, model):
    with path.open(newline='', encoding='utf-8') as f:
        return [model(**row) for row in csv.DictReader(f)]


def test_pipeline_end_to_end():
    base = Path('data/templates')
    budget_actuals = _load_csv(base / 'budget_actuals.csv', BudgetActualRow)
    change_orders = _load_csv(base / 'change_orders.csv', ChangeOrderRow)
    vendor_map = _load_csv(base / 'vendor_map.csv', VendorMapRow)
    category_map = _load_csv(base / 'category_map.csv', CategoryMapRow)

    cfg = ConfigModel()
    cat_lu = build_category_lookup(category_map)
    items = group_variances(budget_actuals, cat_lu)
    attach_drivers_and_vendors(items, change_orders, vendor_map, cat_lu)
    material = filter_materiality(items, cfg)

    assert len(material) == 2
    materials = [v for v in material if v.category == 'Materials'][0]
    assert materials.variance_sar == 200000.0
    assert materials.drivers == ["CO-014: Façade upgrade"]
    assert materials.vendors == ["Al Noor Façade Systems LLC"]


def test_generate_drafts_change_orders_only():
    change_orders = [
        ChangeOrderRow(
            project_id="P1",
            co_id="CO1",
            date="2024-03-10",
            amount_sar=150000,
            description="Extra scope",
            linked_cost_code="100-200",
        )
    ]
    category_map = [CategoryMapRow(cost_code="100-200", category="Materials")]
    req = DraftRequest(
        budget_actuals=[],
        change_orders=change_orders,
        vendor_map=[],
        category_map=category_map,
        config=ConfigModel(materiality_pct=0, materiality_amount_sar=0),
    )
    summary = generate_drafts(req)
    assert summary["kind"] == "summary"
    assert summary["insights"]["total_change_orders"] == 1
    assert summary["insights"]["total_amount_sar"] == 150000.0


def test_generate_drafts_no_budget_actual_pairs_returns_summary():
    budget_actuals = [
        BudgetActualRow(
            project_id="P1",
            period="2024-01",
            cost_code="100-200",
            category="Materials",
            budget_sar=1000,
            actual_sar=0,
        )
    ]
    req = DraftRequest(
        budget_actuals=budget_actuals,
        change_orders=[],
        vendor_map=[],
        category_map=[],
        config=ConfigModel(materiality_pct=0, materiality_amount_sar=0),
    )
    summary = generate_drafts(req)
    assert summary["kind"] == "summary"
    assert summary["insights"].get("row_count") == 1
