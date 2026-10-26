"""src/ingest.py against a real local Spark + Delta engine."""
from datetime import date, datetime
from decimal import Decimal

import pytest

from src import ingest
from tests.conftest import DAY1, DAY2, write_csv

CUST_HEADER = "customer_id,email,first_name,last_name,country,created_at,updated_at\n"
PROD_HEADER = "product_id,name,category,unit_price,is_active,updated_at\n"
TS = "2025-01-01 00:00:00"


def _cust(*rows):
    return CUST_HEADER + "".join(r + "\n" for r in rows)


def _rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


# --- ensure_objects --------------------------------------------------------

def test_ensure_objects_creates_schemas_and_tables(spark, cfg, clean):
    ingest.ensure_objects(spark, cfg)
    ingest.ensure_objects(spark, cfg)  # idempotent
    for name in cfg.tables:
        schema = spark.table(cfg.table_fqn(name)).schema
        assert schema.names == ingest.target_columns(cfg, cfg.tables[name])
        assert schema["_source_row"].dataType.simpleString() == "bigint"
    assert not spark.table(cfg.table_fqn("customers")).schema["customer_id"].nullable
    assert spark.table(cfg.rejects_fqn).count() == 0
    assert spark.table(cfg.audit_fqn).count() == 0


# --- read_source -----------------------------------------------------------

def test_read_source_numbers_rows_and_records_file(spark, cfg, landing):
    path = write_csv(landing, "customers", DAY1, _cust(
        f"1,a@x,A,A,US,{TS},{TS}", f"2,b@x,B,B,US,{TS},{TS}", f"3,c@x,C,C,US,{TS},{TS}"))
    df = ingest.read_source(spark, cfg, cfg.tables["customers"], DAY1)
    rows = _rows(df, "_source_row", "customer_id", "_source_file")
    assert [r[:2] for r in rows] == [(1, "1"), (2, "2"), (3, "3")]
    assert all(r[2].endswith(str(path)) for r in rows)
    assert dict(df.dtypes)["customer_id"] == "string"


def test_read_source_numbers_rows_per_file(spark, cfg, landing):
    write_csv(landing, "customers", DAY1, _cust(f"1,a@x,A,A,US,{TS},{TS}"), name="customers_a.csv")
    write_csv(landing, "customers", DAY1, _cust(f"2,b@x,B,B,US,{TS},{TS}", f"3,c@x,C,C,US,{TS},{TS}"),
              name="customers_b.csv")
    df = ingest.read_source(spark, cfg, cfg.tables["customers"], DAY1)
    got = sorted((r["_source_file"].rsplit("/", 1)[1], r["_source_row"], r["customer_id"]) for r in df.collect())
    assert got == [("customers_a.csv", 1, "1"), ("customers_b.csv", 1, "2"), ("customers_b.csv", 2, "3")]


def test_read_source_ignores_files_not_matching_pattern(spark, cfg, landing):
    write_csv(landing, "customers", DAY1, _cust(f"1,a@x,A,A,US,{TS},{TS}"))
    write_csv(landing, "customers", DAY1, "junk\n", name="notes.txt")
    assert ingest.read_source(spark, cfg, cfg.tables["customers"], DAY1).count() == 1


def test_read_source_missing_files(spark, cfg, landing):
    with pytest.raises(ingest.SourceError, match="no files"):
        ingest.read_source(spark, cfg, cfg.tables["customers"], DAY1)


def test_read_source_header_mismatch(spark, cfg, landing):
    write_csv(landing, "customers", DAY1, "customer_id,email\n1,a@x\n")
    with pytest.raises(ingest.SourceError, match="missing"):
        ingest.read_source(spark, cfg, cfg.tables["customers"], DAY1)


def test_read_source_header_in_other_order(spark, cfg, landing):
    write_csv(landing, "products", DAY1,
              "name,product_id,category,unit_price,is_active,updated_at\nWidget,P001,T,1.00,true," + TS + "\n")
    row = ingest.read_source(spark, cfg, cfg.tables["products"], DAY1).collect()[0]
    assert (row["product_id"], row["name"]) == ("P001", "Widget")


# --- validate_rows ---------------------------------------------------------

def _validate(spark, cfg, landing, table, text):
    write_csv(landing, table, DAY1, text)
    raw = ingest.read_source(spark, cfg, cfg.tables[table], DAY1)
    return ingest.validate_rows(raw, cfg, cfg.tables[table])


def test_validate_rows_casts_types(spark, cfg, landing):
    good, bad = _validate(spark, cfg, landing, "products", PROD_HEADER + f"P001,Widget,,9.99,true,{TS}\n")
    assert bad.count() == 0
    row = good.collect()[0]
    assert row["unit_price"] == Decimal("9.99")
    assert row["is_active"] is True
    assert row["category"] is None  # empty string -> NULL
    assert row["updated_at"] == datetime(2025, 1, 1)
    assert row["_source_row"] == 1


def test_validate_rows_reject_reasons(spark, cfg, landing):
    good, bad = _validate(spark, cfg, landing, "products", PROD_HEADER + "".join([
        f"P001,ok,,1.00,true,{TS}\n",
        f",nopk,,1.00,true,{TS}\n",           # 2 not_null:product_id
        f"P003,badts,,1.00,true,yesterday\n",  # 3 cast:updated_at
        f"P004,round,,9.999,true,{TS}\n",      # 4 cast:unit_price (would round)
        f"P005,bool,,1.00,yes,{TS}\n",         # 5 cast:is_active
        "P006,short\n",                        # 6 malformed
        f"P007,,,1.00,true,{TS}\n",            # 7 not_null:name
    ]))
    assert _rows(good, "product_id") == [("P001",)]
    reasons = dict(_rows(bad, "source_row", "reason"))
    assert reasons[2] == "not_null:product_id"
    assert reasons[3] == "cast:updated_at"
    assert reasons[4] == "cast:unit_price"
    assert reasons[5] == "cast:is_active"
    assert reasons[6].startswith("malformed")
    assert reasons[7] == "not_null:name"
    raw4 = [r["raw_record"] for r in bad.collect() if r["source_row"] == 4][0]
    assert '"unit_price":"9.999"' in raw4


def test_validate_rows_multiple_reasons_joined(spark, cfg, landing):
    _, bad = _validate(spark, cfg, landing, "products", PROD_HEADER + ",,,abc,true," + TS + "\n")
    assert bad.collect()[0]["reason"] == "not_null:product_id;not_null:name;cast:unit_price"


def test_validate_rows_allowed_values(spark, cfg, landing):
    header = "order_id,customer_id,product_id,quantity,unit_price,order_status,order_ts,updated_at\n"
    good, bad = _validate(spark, cfg, landing, "orders", header +
                          f"1,1,P1,1,1.00,PLACED,{TS},{TS}\n2,1,P1,1,1.00,LOST,{TS},{TS}\n")
    assert _rows(good, "order_id") == [(1,)]
    assert _rows(bad, "source_row", "reason") == [(2, "allowed_values:order_status")]


def test_validate_rows_int_overflow(spark, cfg, landing):
    _, bad = _validate(spark, cfg, landing, "customers", _cust(f"99999999999,a@x,A,A,US,{TS},{TS}"))
    assert bad.collect()[0]["reason"] == "cast:customer_id"


# --- dedupe ----------------------------------------------------------------

def test_dedupe_keeps_latest_ordering_value(spark, cfg, landing):
    good, _ = _validate(spark, cfg, landing, "customers", _cust(
        "1,new@x,A,A,US,2025-01-01 00:00:00,2025-02-01 00:00:00",
        "1,old@x,A,A,US,2025-01-01 00:00:00,2025-01-01 00:00:00",
        f"2,b@x,B,B,US,{TS},{TS}"))
    out = ingest.dedupe(good, cfg, cfg.tables["customers"])
    assert _rows(out, "customer_id", "email") == [(1, "new@x"), (2, "b@x")]


def test_dedupe_tie_broken_by_later_row(spark, cfg, landing):
    good, _ = _validate(spark, cfg, landing, "customers", _cust(
        f"1,first@x,A,A,US,{TS},{TS}", f"1,second@x,A,A,US,{TS},{TS}"))
    out = ingest.dedupe(good, cfg, cfg.tables["customers"]).collect()
    assert [(r["email"], r["_source_row"]) for r in out] == [("second@x", 2)]


# --- writers ---------------------------------------------------------------

def _typed(spark, cfg, landing, table, text, load_date=DAY1):
    write_csv(landing, table, load_date, text)
    raw = ingest.read_source(spark, cfg, cfg.tables[table], load_date)
    good, _ = ingest.validate_rows(raw, cfg, cfg.tables[table])
    return ingest.with_metadata(ingest.dedupe(good, cfg, cfg.tables[table]), load_date, datetime(2026, 1, 1, 23))


def test_with_metadata(spark, cfg, landing):
    df = _typed(spark, cfg, landing, "customers", _cust(f"1,a@x,A,A,US,{TS},{TS}"))
    row = df.collect()[0]
    assert row["_load_date"] == date(2026, 1, 1)
    assert row["_ingested_at"] == datetime(2026, 1, 1, 23)


def test_write_snapshot_replaces_table(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    products = cfg.tables["products"]
    d1 = _typed(spark, cfg, landing, "products", PROD_HEADER + f"P1,a,,1.00,true,{TS}\nP2,b,,1.00,true,{TS}\n")
    assert ingest.write_snapshot(spark, cfg, products, d1) == {"rows_inserted": 2}
    d2 = _typed(spark, cfg, landing, "products", PROD_HEADER + f"P3,c,,1.00,true,{TS}\n", load_date=DAY2)
    assert ingest.write_snapshot(spark, cfg, products, d2) == {"rows_inserted": 1}
    assert _rows(spark.table(cfg.table_fqn("products")), "product_id") == [("P3",)]


def test_write_merge_inserts_updates_and_ignores_stale(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    customers = cfg.tables["customers"]
    d1 = _typed(spark, cfg, landing, "customers", _cust(
        "1,a@x,A,A,US,2025-01-01 00:00:00,2025-01-05 00:00:00",
        "2,b@x,B,B,US,2025-01-01 00:00:00,2025-01-05 00:00:00"))
    assert ingest.write_merge(spark, cfg, customers, d1) == {"rows_inserted": 2, "rows_updated": 0}
    d2 = _typed(spark, cfg, landing, "customers", _cust(
        "1,a2@x,A,A,US,2025-01-01 00:00:00,2025-01-06 00:00:00",   # newer -> update
        "2,b2@x,B,B,US,2025-01-01 00:00:00,2025-01-04 00:00:00",   # older -> ignored
        "3,c@x,C,C,US,2025-01-01 00:00:00,2025-01-06 00:00:00"), load_date=DAY2)
    assert ingest.write_merge(spark, cfg, customers, d2) == {"rows_inserted": 1, "rows_updated": 1}
    target = spark.table(cfg.table_fqn("customers"))
    assert _rows(target, "customer_id", "email", "_load_date") == [
        (1, "a2@x", date(2026, 1, 2)), (2, "b@x", date(2026, 1, 1)), (3, "c@x", date(2026, 1, 2))]


def test_write_merge_equal_ordering_value_is_not_an_update(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    customers = cfg.tables["customers"]
    d1 = _typed(spark, cfg, landing, "customers", _cust(f"1,a@x,A,A,US,{TS},{TS}"))
    ingest.write_merge(spark, cfg, customers, d1)
    assert ingest.write_merge(spark, cfg, customers, d1) == {"rows_inserted": 0, "rows_updated": 0}


def test_write_rejects_replaces_per_table_and_date(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    write_csv(landing, "customers", DAY1, _cust(f",a@x,A,A,US,{TS},{TS}", f",b@x,B,B,US,{TS},{TS}"))
    raw = ingest.read_source(spark, cfg, cfg.tables["customers"], DAY1)
    _, bad = ingest.validate_rows(raw, cfg, cfg.tables["customers"])
    assert ingest.write_rejects(spark, cfg, cfg.tables["customers"], DAY1, bad) == 2
    assert ingest.write_rejects(spark, cfg, cfg.tables["customers"], DAY1, bad) == 2
    assert spark.table(cfg.rejects_fqn).count() == 2  # replaced, not appended
    assert ingest.write_rejects(spark, cfg, cfg.tables["customers"], DAY1, bad.limit(0)) == 0
    assert spark.table(cfg.rejects_fqn).count() == 0


def test_write_rejects_leaves_other_dates_alone(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    t = cfg.tables["customers"]
    for d in (DAY1, DAY2):
        write_csv(landing, "customers", d, _cust(f",a@x,A,A,US,{TS},{TS}"))
        _, bad = ingest.validate_rows(ingest.read_source(spark, cfg, t, d), cfg, t)
        ingest.write_rejects(spark, cfg, t, d, bad)
    _, bad = ingest.validate_rows(ingest.read_source(spark, cfg, t, DAY2), cfg, t)
    ingest.write_rejects(spark, cfg, t, DAY2, bad.limit(0))
    assert _rows(spark.table(cfg.rejects_fqn), "load_date") == [(date(2026, 1, 1),)]


def test_write_audit_appends(spark, cfg, clean):
    ingest.ensure_objects(spark, cfg)
    rec = ingest.new_audit_record("run1", cfg.tables["customers"], DAY1, datetime(2026, 1, 1))
    rec.update(status="SUCCESS", rows_read=3, finished_at=datetime(2026, 1, 1, 0, 1))
    ingest.write_audit(spark, cfg, rec)
    ingest.write_audit(spark, cfg, rec)
    rows = spark.table(cfg.audit_fqn).collect()
    assert len(rows) == 2 and rows[0]["rows_read"] == 3 and rows[0]["table_name"] == "customers"
    assert rows[0]["rows_updated"] is None


# --- load_table / run_batch ------------------------------------------------

def test_load_table_success_record(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    write_csv(landing, "customers", DAY1, _cust(
        f"1,a@x,A,A,US,{TS},{TS}", f"1,a@x,A,A,US,{TS},{TS}", f",x@x,X,X,US,{TS},{TS}"))
    rec = ingest.load_table(spark, cfg, cfg.tables["customers"], DAY1, "r1")
    assert rec["status"] == "SUCCESS"
    assert (rec["rows_read"], rec["rows_rejected"], rec["duplicates_removed"],
            rec["rows_inserted"], rec["rows_updated"], rec["rows_stale"]) == (3, 1, 1, 1, 0, 0)
    assert spark.table(cfg.audit_fqn).filter("status = 'SUCCESS'").count() == 1


def test_load_table_failure_is_logged_and_raised(spark, cfg, landing, clean):
    ingest.ensure_objects(spark, cfg)
    with pytest.raises(ingest.SourceError):
        ingest.load_table(spark, cfg, cfg.tables["customers"], DAY1, "r1")
    row = spark.table(cfg.audit_fqn).collect()[0]
    assert row["status"] == "FAILED" and "no files" in row["error"]
    assert spark.table(cfg.table_fqn("customers")).count() == 0


def test_run_batch_stops_on_first_failure(spark, cfg, landing, clean):
    write_csv(landing, "customers", DAY1, _cust(f"1,a@x,A,A,US,{TS},{TS}"))
    with pytest.raises(ingest.SourceError):
        ingest.run_batch(spark, cfg, DAY1)  # products has no file
    log = _rows(spark.table(cfg.audit_fqn), "table_name", "status")
    assert log == [("customers", "SUCCESS"), ("products", "FAILED")]


def test_run_batch_rejects_bad_load_date(spark, cfg):
    from src.config import ConfigError
    with pytest.raises(ConfigError):
        ingest.run_batch(spark, cfg, "2026/01/01")
