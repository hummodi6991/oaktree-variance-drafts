import csv
from pathlib import Path

from app.pipeline import (
    build_category_lookup,
    group_variances,
    attach_drivers_and_vendors,
    filter_materiality,
)
from app.schemas import (
    BudgetActualRow,
    ChangeOrderRow,
    VendorMapRow,
    CategoryMapRow,
    ConfigModel,
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
