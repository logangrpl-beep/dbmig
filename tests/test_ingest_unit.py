"""Pure (no Spark) tests for SQL/expression builders in src/ingest.py."""
import pytest

from src import ingest
from src.config import load_config
from tests.conftest import TABLES_YAML


@pytest.fixture
def c():
    return load_config(TABLES_YAML, catalog="cat_x")


def test_table_ddl(c):
    ddl = ingest.table_ddl(c, c.tables["products"])
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `cat_x`.`bronze`.`products`")
    assert "`product_id` STRING NOT NULL" in ddl
    assert "`category` STRING," in ddl
    assert "`unit_price` DECIMAL(10,2) NOT NULL" in ddl
    assert "`_source_row` BIGINT" in ddl
    assert "`_ingested_at` TIMESTAMP" in ddl
    assert ddl.rstrip().endswith("USING DELTA")


def test_rejects_and_audit_ddl(c):
    rejects = ingest.rejects_ddl(c)
    assert "`cat_x`.`ops`.`rejected_rows`" in rejects
    for col in ("table_name", "load_date", "source_file", "source_row", "reason", "raw_record"):
        assert f"`{col}`" in rejects
    audit = ingest.audit_ddl(c)
    assert "`cat_x`.`ops`.`load_log`" in audit
    for col in ("run_id", "status", "rows_read", "rows_stale", "fk_violations", "warnings", "error"):
        assert f"`{col}`" in audit


def test_check_header_ok(c):
    t = c.tables["customers"]
    ingest.check_header(t, list(t.column_names), strict=True)
    ingest.check_header(t, list(reversed(t.column_names)), strict=True)  # order-insensitive


def test_check_header_missing_column(c):
    t = c.tables["customers"]
    with pytest.raises(ingest.SourceError, match="missing.*email"):
        ingest.check_header(t, [n for n in t.column_names if n != "email"], strict=True)


def test_check_header_extra_column(c):
    t = c.tables["customers"]
    with pytest.raises(ingest.SourceError, match="unexpected.*phone"):
        ingest.check_header(t, t.column_names + ["phone"], strict=True)
    # Non-strict tolerates extra columns but never missing ones.
    ingest.check_header(t, t.column_names + ["phone"], strict=False)
    with pytest.raises(ingest.SourceError):
        ingest.check_header(t, t.column_names[1:], strict=False)


def test_check_header_duplicate_column(c):
    t = c.tables["customers"]
    with pytest.raises(ingest.SourceError, match="duplicate"):
        ingest.check_header(t, t.column_names + ["email"], strict=True)


@pytest.mark.parametrize("col_type,expected", [
    ("STRING", "`v`"),
    ("INT", "try_cast(`v` AS INT)"),
    ("BIGINT", "try_cast(`v` AS BIGINT)"),
    ("BOOLEAN", "try_cast(`v` AS BOOLEAN)"),
    ("DECIMAL(10,2)", "try_cast(`v` AS DECIMAL(10,2))"),
    ("TIMESTAMP", "try_to_timestamp(`v`, 'yyyy-MM-dd HH:mm:ss')"),
    ("DATE", "CAST(try_to_timestamp(`v`, 'yyyy-MM-dd') AS DATE)"),
])
def test_cast_sql(c, col_type, expected):
    assert ingest.cast_sql("v", col_type, c.csv_options) == expected


def test_format_check_sql():
    assert ingest.format_check_sql("v", "DECIMAL(10,2)") == "`v` RLIKE '^[+-]?[0-9]{1,8}(\\\\.[0-9]{1,2})?$'"
    assert ingest.format_check_sql("v", "BOOLEAN") == "lower(`v`) IN ('true', 'false')"
    assert ingest.format_check_sql("v", "DECIMAL(5,0)") == "`v` RLIKE '^[+-]?[0-9]{1,5}$'"
    assert ingest.format_check_sql("v", "INT") == "`v` RLIKE '^[+-]?[0-9]+$'"
    assert ingest.format_check_sql("v", "BIGINT") == "`v` RLIKE '^[+-]?[0-9]+$'"
    assert ingest.format_check_sql("v", "STRING") is None
    assert ingest.format_check_sql("v", "TIMESTAMP") is None


def test_merge_sql(c):
    sql = ingest.merge_sql(c, c.tables["orders"], "src_view")
    assert sql.startswith("MERGE INTO `cat_x`.`bronze`.`orders` AS t USING `src_view` AS s")
    assert "ON t.`order_id` = s.`order_id`" in sql
    assert "WHEN MATCHED AND s.`updated_at` > t.`updated_at` THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "DELETE" not in sql


def test_merge_sql_composite_key(c):
    t = c.tables["orders"]
    t2 = type(t)(**{**t.__dict__, "primary_key": ["order_id", "customer_id"]})
    sql = ingest.merge_sql(c, t2, "v")
    assert "ON t.`order_id` = s.`order_id` AND t.`customer_id` = s.`customer_id`" in sql


def test_snapshot_sql(c):
    sql = ingest.snapshot_sql(c, c.tables["products"], "v")
    assert sql.startswith("INSERT OVERWRITE TABLE `cat_x`.`bronze`.`products` SELECT `product_id`, ")
    assert sql.endswith("`_ingested_at` FROM `v`")


def test_target_columns(c):
    cols = ingest.target_columns(c, c.tables["customers"])
    assert cols[0] == "customer_id" and cols[-4:] == ["_source_file", "_source_row", "_load_date", "_ingested_at"]
