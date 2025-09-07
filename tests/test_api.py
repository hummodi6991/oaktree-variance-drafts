import csv
import os
from pathlib import Path

os.environ["API_KEY"] = "testkey"
os.environ["REQUIRE_API_KEY"] = "true"

from fastapi.testclient import TestClient

from app.main import app


def _load_csv(path: Path):
    with path.open(newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def test_create_drafts_endpoint(dummy_llm):
    base = Path('data/templates')
    payload = {
        'budget_actuals': _load_csv(base / 'budget_actuals.csv'),
        'change_orders': _load_csv(base / 'change_orders.csv'),
        'vendor_map': _load_csv(base / 'vendor_map.csv'),
        'category_map': _load_csv(base / 'category_map.csv'),
        'config': {
            'materiality_pct': 5.0,
            'materiality_amount_sar': 100000,
            'bilingual': True,
            'enforce_no_speculation': True,
        }
    }

    client = TestClient(app)
    resp = client.post('/drafts', json=payload, headers={"x-api-key": "testkey"})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data.get('variances'), list)
    assert len(data['variances']) == 2
    assert all('draft_en' in item for item in data['variances'])
    assert '_meta' in data
