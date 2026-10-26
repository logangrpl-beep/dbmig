"""End-to-end scenario from design/test_spec.md: day 1, day 2, re-runs."""
import pytest

from scripts import generate_data
from src import ingest, validate
from src.config import load_config
from tests.conftest import DAY1, DAY2, EXPECTED_YAML, LOCAL_CATALOG, TABLES_YAML

BUSINESS = {"_ingested_at"}  # only column allowed to change on an idempotent re-run


@pytest.fixture(scope="module")
def env(spark, tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    generate_data.generate(landing)
    c = load_config(TABLES_YAML, catalog=LOCAL_CATALOG, source_path=str(landing) + "/${table}/${load_date}/")
    for schema in c.schemas():
        spark.sql(f"DROP SCHEMA IF EXISTS {c.schema_fqn(schema)} CASCADE")
    return c


def _snapshot(spark, c):
    out = {}
    for name in c.tables:
        df = spark.table(c.table_fqn(name))
        cols = [x for x in df.columns if x not in BUSINESS]
        out[name] = sorted(tuple(r) for r in df.select(cols).collect())
    return out


def _assert_checks(spark, c, load_date):
    results = validate.run_checks(spark, c, load_date, validate.load_expected(EXPECTED_YAML, load_date))
    failed = [r for r in results if not r.passed]
    assert not failed, validate.format_results(failed)
    return results


@pytest.fixture(scope="module")
def after_day1(spark, env):
    records = ingest.run_batch(spark, env, DAY1)
    return records, _snapshot(spark, env)


@pytest.fixture(scope="module")
def after_day2(spark, env, after_day1):
    records = ingest.run_batch(spark, env, DAY2)
    return records, _snapshot(spark, env)


def test_day1_records(after_day1):
    records, _ = after_day1
    got = {r["table_name"]: (r["rows_read"], r["rows_rejected"], r["duplicates_removed"], r["rows_inserted"])
           for r in records}
    assert got == {"customers": (7, 1, 1, 5), "products": (5, 0, 1, 4), "orders": (11, 1, 2, 8)}
    assert all(r["status"] == "SUCCESS" for r in records)


def test_day1_acceptance(spark, env, after_day1):
    _assert_checks(spark, env, DAY1)


def test_day2_records(after_day2):
    records, _ = after_day2
    got = {r["table_name"]: (r["rows_inserted"], r["rows_updated"], r["rows_stale"]) for r in records}
    assert got == {"customers": (2, 1, 1), "products": (4, None, None), "orders": (4, 2, 0)}


def test_day2_acceptance(spark, env, after_day2):
    _assert_checks(spark, env, DAY2)


def test_day2_fk_warning_logged_not_failed(spark, env, after_day2):
    records, _ = after_day2
    orders = [r for r in records if r["table_name"] == "orders"][0]
    assert orders["status"] == "SUCCESS"
    assert orders["fk_violations"] == 1
    assert "product_id->products.product_id=1" in orders["warnings"]


def test_day2_untouched_orders_unchanged(after_day1, after_day2):
    # M6: orders 1001, 1004-1008 identical (all columns incl. metadata) to day 1.
    keep = {1001, 1004, 1005, 1006, 1007, 1008}
    d1 = [r for r in after_day1[1]["orders"] if r[0] in keep]
    d2 = [r for r in after_day2[1]["orders"] if r[0] in keep]
    assert d1 == d2 and len(d2) == 6


def test_day2_all_products_from_day2(spark, env, after_day2):
    # P4
    dates = {r[0] for r in spark.table(env.table_fqn("products")).select("_load_date").collect()}
    assert [str(d) for d in dates] == [DAY2]


def test_rerun_day2_is_idempotent(spark, env, after_day2):
    # I1, I2, I4
    log_before = spark.table(env.audit_fqn).count()
    records = ingest.run_batch(spark, env, DAY2)
    assert _snapshot(spark, env) == after_day2[1]
    _assert_checks(spark, env, DAY2)
    assert spark.table(env.rejects_fqn).filter(f"load_date = DATE'{DAY1}'").count() == 2
    assert spark.table(env.rejects_fqn).filter(f"load_date = DATE'{DAY2}'").count() == 0
    assert spark.table(env.audit_fqn).count() == log_before + 3
    got = {r["table_name"]: (r["rows_inserted"], r["rows_updated"], r["rows_stale"]) for r in records}
    assert got == {"customers": (0, 0, 4), "products": (4, None, None), "orders": (0, 0, 6)}


def test_day1_twice_on_empty_catalog(spark, env, after_day2):
    # I3 (runs last in this module: it resets the catalog)
    for schema in env.schemas():
        spark.sql(f"DROP SCHEMA IF EXISTS {env.schema_fqn(schema)} CASCADE")
    ingest.run_batch(spark, env, DAY1)
    first = _snapshot(spark, env)
    ingest.run_batch(spark, env, DAY1)
    assert _snapshot(spark, env) == first
    _assert_checks(spark, env, DAY1)
