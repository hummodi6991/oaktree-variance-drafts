import io
import pandas as pd
from app.services.singlefile import process_single_file


def _build_workbook():
    line_items = pd.DataFrame({
        'vendor_name': ['VendorA', 'VendorB'],
        'item_code': ['D01', 'D01'],
        'description of works': ['Door', 'Door'],
        'qty': [1, 1],
        'unit price (sar)': [100, 120],
        'total price (sar)': [100, 120],
    })
    vendor_totals = pd.DataFrame({
        'Vendor': ['VendorA', 'VendorB'],
        'Total_SAR': [100, 120],
    })
    highlights = pd.DataFrame({
        'item_code': ['D01'],
        'qty': [1],
        'best_unit_rate_sar': [100],
        'note': ['fast delivery'],
    })
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine='openpyxl') as xl:
        line_items.to_excel(xl, index=False, sheet_name='line_items')
        vendor_totals.to_excel(xl, index=False, sheet_name='price_comparison_totals')
        highlights.to_excel(xl, index=False, sheet_name='price_comp_highlights')
    return bio.getvalue()


def test_doors_quotes_adapter_handles_workbook():
    data = _build_workbook()
    resp = process_single_file('doors_quotes.xlsx', data)
    assert resp['mode'] == 'quote_compare'
    assert resp['items_rowcount'] == 2
    assert resp['variance_items'], 'expected variance items'
    assert len(resp['vendor_totals']) == 2
    assert resp.get('insights', {}).get('highlights')
