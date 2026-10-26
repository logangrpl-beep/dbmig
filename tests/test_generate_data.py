import csv

import pytest

from scripts import generate_data
from src.config import load_config
from tests.conftest import DAY1, DAY2, TABLES_YAML

# Data-row counts per design/test_spec.md §2 ("Raw rows read").
RAW_ROWS = {
    (DAY1, "customers"): 7, (DAY1, "products"): 5, (DAY1, "orders"): 11,
    (DAY2, "customers"): 4, (DAY2, "products"): 4, (DAY2, "orders"): 6,
}


def _read(path):
    with open(path, newline="") as f:
        return list(csv.reader(f))


def test_generate_writes_every_table_and_day(tmp_path):
    paths = generate_data.generate(tmp_path)
    assert len(paths) == 6
    for (load_date, table), n in RAW_ROWS.items():
        path = tmp_path / table / load_date / f"{table}_{load_date.replace('-', '')}.csv"
        assert path in paths
        assert len(_read(path)) == n + 1  # + header


def test_headers_match_tables_yaml(tmp_path):
    cfg = load_config(TABLES_YAML, catalog="c")
    generate_data.generate(tmp_path)
    for (load_date, table) in RAW_ROWS:
        header = _read(next((tmp_path / table / load_date).glob("*.csv")))[0]
        assert header == cfg.tables[table].column_names


def test_generate_single_day(tmp_path):
    paths = generate_data.generate(tmp_path, days=[DAY2])
    assert len(paths) == 3
    assert not (tmp_path / "orders" / DAY1).exists()


def test_generate_unknown_day(tmp_path):
    with pytest.raises(ValueError):
        generate_data.generate(tmp_path, days=["2030-01-01"])


def test_scenario_edge_cases_present(tmp_path):
    generate_data.generate(tmp_path)
    customers_d1 = _read(tmp_path / "customers" / DAY1 / "customers_20260101.csv")[1:]
    assert customers_d1[6][0] == ""  # NULL PK row (row 7)
    assert [r[0] for r in customers_d1].count("3") == 2

    orders_d1 = _read(tmp_path / "orders" / DAY1 / "orders_20260101.csv")[1:]
    dup_1005 = [r for r in orders_d1 if r[0] == "1005"]
    assert len(dup_1005) == 2 and dup_1005[0] == dup_1005[1]
    assert orders_d1[10][5] == "LOST"  # invalid status (row 11)

    products_d2 = [r[0] for r in _read(tmp_path / "products" / DAY2 / "products_20260102.csv")[1:]]
    assert products_d2 == ["P001", "P002", "P003", "P005"]


def test_generate_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    for p, q in zip(generate_data.generate(a), generate_data.generate(b)):
        assert p.read_bytes() == q.read_bytes()


def test_main_cli(tmp_path, capsys):
    assert generate_data.main(["--out", str(tmp_path), "--day", DAY1]) == 0
    assert len(list(tmp_path.rglob("*.csv"))) == 3
    assert "customers_20260101.csv" in capsys.readouterr().out
