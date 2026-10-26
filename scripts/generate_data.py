"""Write the day-1 / day-2 CSV exports described in design/test_spec.md §1.

Layout matches defaults.source.path in tables.yaml:
    <out>/<table>/<load_date>/<table>_<yyyymmdd>.csv

Upload to the landing volume with e.g.
    databricks fs cp -r data/landing dbfs:/Volumes/<catalog>/landing/raw --overwrite
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

HEADERS = {
    "customers": ["customer_id", "email", "first_name", "last_name", "country", "created_at", "updated_at"],
    "products": ["product_id", "name", "category", "unit_price", "is_active", "updated_at"],
    "orders": ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_status", "order_ts",
               "updated_at"],
}

# Rows are verbatim from test_spec.md; "" is an empty CSV field (NULL).
SCENARIO = {
    "2026-01-01": {
        "customers": [
            ["1", "alice@ex.com", "Alice", "Ng", "US", "2025-06-01 08:00:00", "2025-06-01 08:00:00"],
            ["2", "bob@ex.com", "Bob", "Lee", "GB", "2025-06-02 08:00:00", "2025-06-02 08:00:00"],
            ["3", "carol@old.com", "Carol", "Diaz", "ES", "2025-06-03 08:00:00", "2025-06-03 08:00:00"],
            ["3", "carol@new.com", "Carol", "Diaz", "ES", "2025-06-03 08:00:00", "2025-12-31 10:00:00"],
            ["4", "dan@ex.com", "Dan", "Kim", "KR", "2025-06-04 08:00:00", "2025-12-01 12:00:00"],
            ["5", "eve@ex.com", "Eve", "Roe", "DE", "2025-06-05 08:00:00", "2025-06-05 08:00:00"],
            ["", "nopk@ex.com", "No", "Key", "US", "2025-06-06 08:00:00", "2025-06-06 08:00:00"],
        ],
        "products": [
            ["P001", "Widget", "Tools", "9.99", "true", "2025-11-01 00:00:00"],
            ["P002", "Gadget", "Tools", "19.99", "true", "2025-12-01 00:00:00"],
            ["P002", "Gadget", "Tools", "24.99", "true", "2025-12-15 00:00:00"],
            ["P003", "Gizmo", "Toys", "4.50", "true", "2025-11-01 00:00:00"],
            ["P004", "Doohickey", "Misc", "14.00", "true", "2025-11-01 00:00:00"],
        ],
        "orders": [
            ["1001", "1", "P001", "2", "9.99", "DELIVERED", "2025-12-20 10:00:00", "2025-12-28 10:00:00"],
            ["1002", "2", "P002", "1", "24.99", "SHIPPED", "2025-12-29 10:00:00", "2025-12-31 10:00:00"],
            ["1003", "3", "P003", "3", "4.50", "PLACED", "2026-01-01 08:00:00", "2026-01-01 08:00:00"],
            ["1003", "3", "P003", "3", "4.50", "SHIPPED", "2026-01-01 08:00:00", "2026-01-01 15:00:00"],
            ["1004", "4", "P001", "1", "9.99", "PLACED", "2026-01-01 09:00:00", "2026-01-01 09:00:00"],
            ["1005", "5", "P002", "4", "24.99", "PLACED", "2026-01-01 09:30:00", "2026-01-01 09:30:00"],
            ["1005", "5", "P002", "4", "24.99", "PLACED", "2026-01-01 09:30:00", "2026-01-01 09:30:00"],
            ["1006", "1", "P003", "2", "4.50", "PLACED", "2026-01-01 10:00:00", "2026-01-01 10:00:00"],
            ["1007", "2", "P001", "1", "9.99", "CANCELLED", "2026-01-01 11:00:00", "2026-01-01 12:00:00"],
            ["1008", "3", "P004", "5", "14.00", "PLACED", "2026-01-01 13:00:00", "2026-01-01 13:00:00"],
            ["1099", "1", "P001", "1", "9.99", "LOST", "2026-01-01 14:00:00", "2026-01-01 14:00:00"],
        ],
    },
    "2026-01-02": {
        "customers": [
            ["2", "bob@new.com", "Bob", "Lee", "GB", "2025-06-02 08:00:00", "2026-01-02 09:00:00"],
            ["6", "frank@ex.com", "Frank", "Moreau", "FR", "2026-01-02 07:00:00", "2026-01-02 07:00:00"],
            ["7", "grace@ex.com", "Grace", "Rossi", "IT", "2026-01-02 08:00:00", "2026-01-02 08:00:00"],
            ["4", "dan@stale.com", "Dan", "Kim", "KR", "2025-06-04 08:00:00", "2025-11-01 12:00:00"],
        ],
        "products": [
            ["P001", "Widget", "Tools", "9.99", "true", "2025-11-01 00:00:00"],
            ["P002", "Gadget", "Tools", "21.99", "true", "2026-01-02 00:00:00"],
            ["P003", "Gizmo", "Toys", "4.50", "true", "2025-11-01 00:00:00"],
            ["P005", "Thingamajig", "Misc", "30.00", "true", "2026-01-02 00:00:00"],
        ],
        "orders": [
            ["1002", "2", "P002", "1", "24.99", "DELIVERED", "2025-12-29 10:00:00", "2026-01-02 10:00:00"],
            ["1003", "3", "P003", "3", "4.50", "DELIVERED", "2026-01-01 08:00:00", "2026-01-02 11:00:00"],
            ["1009", "6", "P005", "1", "30.00", "PLACED", "2026-01-02 08:00:00", "2026-01-02 08:00:00"],
            ["1010", "7", "P001", "2", "9.99", "PLACED", "2026-01-02 09:00:00", "2026-01-02 09:00:00"],
            ["1011", "2", "P005", "3", "30.00", "PLACED", "2026-01-02 10:00:00", "2026-01-02 10:00:00"],
            ["1012", "1", "P002", "1", "21.99", "PLACED", "2026-01-02 11:00:00", "2026-01-02 11:00:00"],
        ],
    },
}


def generate(out_dir: str | Path, days: list[str] | None = None) -> list[Path]:
    """Write the scenario CSVs under out_dir; returns the written paths."""
    out_dir = Path(out_dir)
    days = list(SCENARIO) if days is None else days
    unknown = [d for d in days if d not in SCENARIO]
    if unknown:
        raise ValueError(f"unknown day(s) {unknown}; choose from {list(SCENARIO)}")
    paths = []
    for load_date in days:
        for table, rows in SCENARIO[load_date].items():
            path = out_dir / table / load_date / f"{table}_{load_date.replace('-', '')}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", newline="") as f:
                writer = csv.writer(f, lineterminator="\n")
                writer.writerow(HEADERS[table])
                writer.writerows(rows)
            paths.append(path)
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data/landing", help="output root (default: data/landing)")
    parser.add_argument("--day", action="append", choices=list(SCENARIO),
                        help="load_date to generate (repeatable; default: all)")
    args = parser.parse_args(argv)
    for path in generate(args.out, args.day):
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
