from datetime import date, datetime
from decimal import Decimal

import pytest

from scripts import generate_data
from src import ingest, validate
from src.config import load_config
from tests.conftest import DAY1, DAY2, EXPECTED_YAML, LOCAL_CATALOG, TABLES_YAML


@pytest.fixture(scope="module")
def loaded(spark, tmp_path_factory):
    """Day 1 of the scenario loaded into a fresh catalog."""
    landing = tmp_path_factory.mktemp("landing")
    generate_data.generate(landing)
    c = load_config(TABLES_YAML, catalog=LOCAL_CATALOG, source_path=str(landing) + "/${table}/${load_date}/")
    for schema in c.schemas():
        spark.sql(f"DROP SCHEMA IF EXISTS {c.schema_fqn(schema)} CASCADE")
    ingest.run_batch(spark, c, DAY1)
    return c


def _by_id(results):
    out = {}
    for r in results:
        out.setdefault(r.id, []).append(r)
    return out


def test_normalize():
    assert validate.normalize(True) == "true"
    assert validate.normalize(Decimal("24.99")) == "24.99"
    assert validate.normalize(datetime(2026, 1, 2, 9)) == "2026-01-02 09:00:00"
    assert validate.normalize(date(2026, 1, 2)) == "2026-01-02"
    assert validate.normalize(None) is None
    assert validate.normalize(3) == "3"


def test_load_expected():
    exp = validate.load_expected(EXPECTED_YAML, DAY2)
    assert {c["table"]: c["expected"] for c in exp["counts"]} == {"customers": 7, "products": 4, "orders": 12}
    with pytest.raises(KeyError):
        validate.load_expected(EXPECTED_YAML, "2030-01-01")


def test_table_exists(spark, loaded):
    assert validate.table_exists(spark, loaded.table_fqn("orders"))
    assert not validate.table_exists(spark, loaded.table_fqn("nope"))


def test_fk_violations(spark, loaded):
    assert validate.fk_violations(spark, loaded, loaded.tables["orders"]) == {
        "customer_id->customers.customer_id": 0,
        "product_id->products.product_id": 0,
    }
    assert validate.fk_violations(spark, loaded, loaded.tables["customers"]) == {}


def test_structural_checks_pass(spark, loaded):
    results = validate.structural_checks(spark, loaded)
    assert {r.id for r in results} == {"S1", "S2", "S3", "S4", "S5"}
    assert all(r.passed for r in results), [r for r in results if not r.passed]


def test_structural_checks_detect_duplicate_pk(spark, loaded):
    fqn = loaded.table_fqn("customers")
    spark.sql(f"INSERT INTO {fqn} SELECT * FROM {fqn} WHERE customer_id = 1")
    try:
        s4 = [r for r in validate.structural_checks(spark, loaded) if r.id == "S4" and "customers" in r.description]
        assert not s4[0].passed and s4[0].actual == "1 duplicate key(s)"
    finally:
        spark.sql(f"DELETE FROM {fqn} WHERE customer_id = 1")
        spark.sql(f"INSERT INTO {fqn} SELECT 1, 'alice@ex.com', 'Alice', 'Ng', 'US', "
                  "TIMESTAMP'2025-06-01 08:00:00', TIMESTAMP'2025-06-01 08:00:00', 'restored', 1, "
                  "DATE'2026-01-01', current_timestamp()")


def test_structural_checks_detect_missing_table(spark, tmp_path):
    c = load_config(TABLES_YAML, catalog=LOCAL_CATALOG, source_path=str(tmp_path) + "/${table}/${load_date}/")
    other = type(c)(**{**c.__dict__, "target_schema": "no_such_schema"})
    s1 = [r for r in validate.structural_checks(spark, other) if r.id == "S1"]
    assert s1 and not any(r.passed for r in s1)


def test_scenario_checks_day1_pass(spark, loaded):
    results = validate.scenario_checks(spark, loaded, DAY1, validate.load_expected(EXPECTED_YAML, DAY1))
    ids = _by_id(results)
    for check_id in ("C1", "C2", "C3", "C4", "C6", "C7", "D1", "D2", "D3", "R1", "R2", "R3", "F1", "F2"):
        assert check_id in ids, check_id
    assert all(r.passed for r in results), [r for r in results if not r.passed]


def test_scenario_checks_report_failures(spark, loaded):
    wrong = {
        "counts": [{"id": "C1", "table": "customers", "expected": 99}],
        "rejected": 5,
        "values": [{"id": "D1", "table": "customers", "key": {"customer_id": 3}, "expect": {"email": "x"}}],
        "absent": [{"id": "R3", "table": "customers", "key": {"customer_id": 1}}],
        "key_set": [{"id": "P1", "table": "products", "keys": ["P001"]}],
        "sums": [{"id": "C7", "table": "orders", "column": "quantity", "expected": "1"}],
        "rejects": [{"id": "R1", "table": "customers", "source_row": 99, "reason": "x"}],
        "fk": [{"id": "F1", "table": "orders", "fk": "customer_id->customers.customer_id", "expected": 7}],
        "load_log": {"customers": {"rows_read": 1}},
    }
    results = validate.scenario_checks(spark, loaded, DAY1, wrong)
    assert results and not any(r.passed for r in results)
    d1 = _by_id(results)["D1"][0]
    assert d1.expected == "x" and d1.actual == "carol@new.com"


def test_scenario_checks_missing_key_fails(spark, loaded):
    exp = {"values": [{"id": "D1", "table": "customers", "key": {"customer_id": 404}, "expect": {"email": "x"}}]}
    r = validate.scenario_checks(spark, loaded, DAY1, exp)[0]
    assert not r.passed and r.actual == "0 rows"


def test_run_checks_includes_latest_run_check(spark, loaded):
    results = validate.run_checks(spark, loaded, DAY1, validate.load_expected(EXPECTED_YAML, DAY1))
    c5 = _by_id(results)["C5"][0]
    assert c5.passed and c5.actual == "3"
    assert all(r.passed for r in results)


def test_run_checks_without_expected(spark, loaded):
    results = validate.run_checks(spark, loaded, DAY1)
    assert {r.id for r in results} == {"S1", "S2", "S3", "S4", "S5", "C5"}


def test_assert_all_passed():
    ok = validate.CheckResult("X", "fine", True, "1", "1")
    bad = validate.CheckResult("Y", "broken", False, "1", "2")
    validate.assert_all_passed([ok])
    with pytest.raises(validate.ValidationError, match="Y"):
        validate.assert_all_passed([ok, bad])


def test_format_results():
    text = validate.format_results([validate.CheckResult("X", "desc", False, "1", "2")])
    assert "FAIL" in text and "X" in text and "desc" in text and "expected=1" in text and "actual=2" in text
