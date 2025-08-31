#!/usr/bin/env python3
import csv
import json
import os
import sys

import requests

API = os.getenv("MVP_API", "http://localhost:8000/drafts")


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main(folder):
    payload = {
        "budget_actuals": read_csv(os.path.join(folder, "budget_actuals.csv")),
        "change_orders": read_csv(os.path.join(folder, "change_orders.csv")),
        "vendor_map": read_csv(os.path.join(folder, "vendor_map.csv")),
        "category_map": read_csv(os.path.join(folder, "category_map.csv")),
        "config": {
            "materiality_pct": 5.0,
            "materiality_amount_sar": 100000,
            "bilingual": True,
            "enforce_no_speculation": True,
        },
    }
    r = requests.post(API, json=payload, timeout=30)
    r.raise_for_status()
    print(json.dumps(r.json(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "data/templates"
    main(folder)
