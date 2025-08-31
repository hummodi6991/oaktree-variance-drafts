#!/usr/bin/env python3
import csv
import os
import sys
from typing import List, Dict

import pandas as pd
import requests

API = os.getenv("MVP_API", "http://localhost:8000/drafts")


def read_table(folder: str, name: str) -> List[Dict]:
    """Read ``name`` with .csv/.xls/.xlsx extension from ``folder``."""
    for ext in (".csv", ".xls", ".xlsx"):
        path = os.path.join(folder, name + ext)
        if os.path.exists(path):
            if ext == ".csv":
                with open(path, newline="", encoding="utf-8") as f:
                    return list(csv.DictReader(f))
            df = pd.read_excel(path)
            return df.fillna("").to_dict(orient="records")
    raise FileNotFoundError(f"Missing file for {name} (csv/xls/xlsx)")


def pretty_print(result: List[Dict]) -> None:
    for i, item in enumerate(result, 1):
        v = item.get("variance", {})
        print(
            f"{i}. Project {v.get('project_id')} | {v.get('period')} | {v.get('category')}"
        )
        print(
            f"   Budget: {v.get('budget_sar')} | Actual: {v.get('actual_sar')} | "
            f"Variance: {v.get('variance_sar')} ({v.get('variance_pct'):.2f}%)"
        )
        print(f"   EN: {item.get('draft_en')}")
        if item.get("draft_ar"):
            print(f"   AR: {item.get('draft_ar')}")
        if item.get("analyst_notes"):
            print(f"   Notes: {item.get('analyst_notes')}")
        print()


def main(folder: str) -> None:
    payload = {
        "budget_actuals": read_table(folder, "budget_actuals"),
        "change_orders": read_table(folder, "change_orders"),
        "vendor_map": read_table(folder, "vendor_map"),
        "category_map": read_table(folder, "category_map"),
        "config": {
            "materiality_pct": 5.0,
            "materiality_amount_sar": 100000,
            "bilingual": True,
            "enforce_no_speculation": True,
        },
    }
    r = requests.post(API, json=payload, timeout=30)
    r.raise_for_status()
    pretty_print(r.json())


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "data/templates"
    main(folder)
