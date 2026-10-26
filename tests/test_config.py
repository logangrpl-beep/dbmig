from datetime import date

import pytest
import yaml

from src.config import (
    Column,
    Config,
    ConfigError,
    ForeignKey,
    TableSpec,
    decimal_precision,
    load_config,
    parse_load_date,
    quote_ident,
    sql_string,
)
from tests.conftest import TABLES_YAML


def _raw():
    return yaml.safe_load(TABLES_YAML.read_text())


def _write(tmp_path, data):
    path = tmp_path / "tables.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_load_config_reads_all_tables():
    cfg = load_config(TABLES_YAML, catalog="cat_a")
    assert isinstance(cfg, Config)
    assert cfg.load_order == ["customers", "products", "orders"]
    assert set(cfg.tables) == {"customers", "products", "orders"}

    customers = cfg.tables["customers"]
    assert customers.primary_key == ["customer_id"]
    assert customers.load_type == "merge_upsert"
    assert customers.ordering_column == "updated_at"
    assert customers.column_names[:2] == ["customer_id", "email"]

    products = cfg.tables["products"]
    assert products.load_type == "snapshot_overwrite"
    assert products.column("unit_price").type == "DECIMAL(10,2)"

    orders = cfg.tables["orders"]
    assert orders.column("order_status").allowed_values == ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
    assert [(fk.columns, fk.ref_table) for fk in orders.foreign_keys] == [
        (["customer_id"], "customers"),
        (["product_id"], "products"),
    ]


def test_defaults_are_parsed():
    cfg = load_config(TABLES_YAML, catalog="cat_a")
    assert cfg.csv_options["header"] is True
    assert cfg.strict_header is True
    assert [c.name for c in cfg.metadata_columns] == ["_source_file", "_source_row", "_load_date", "_ingested_at"]
    assert cfg.dedupe_tiebreak == [("_source_file", "desc"), ("_source_row", "desc")]


def test_catalog_is_substituted_everywhere():
    cfg = load_config(TABLES_YAML, catalog="other_cat")
    assert cfg.table_fqn("orders") == "`other_cat`.`bronze`.`orders`"
    assert cfg.rejects_fqn == "`other_cat`.`ops`.`rejected_rows`"
    assert cfg.audit_fqn == "`other_cat`.`ops`.`load_log`"
    assert cfg.schema_fqn("ops") == "`other_cat`.`ops`"
    assert cfg.source_dir("orders", "2026-01-02") == "/Volumes/other_cat/landing/raw/orders/2026-01-02/"
    assert cfg.source_glob("orders", "2026-01-02") == "/Volumes/other_cat/landing/raw/orders/2026-01-02/orders_*.csv"


def test_schemas_lists_target_and_ops():
    cfg = load_config(TABLES_YAML, catalog="c")
    assert cfg.schemas() == ["bronze", "ops"]


def test_source_path_override():
    cfg = load_config(TABLES_YAML, catalog="c", source_path="/tmp/land/${table}/${load_date}")
    assert cfg.source_dir("products", "2026-01-01") == "/tmp/land/products/2026-01-01/"


@pytest.mark.parametrize("catalog", ["", None, "bad name", "a.b", "x;drop"])
def test_invalid_catalog_rejected(catalog):
    with pytest.raises(ConfigError):
        load_config(TABLES_YAML, catalog=catalog)


def test_catalog_from_env(monkeypatch):
    monkeypatch.setenv("DBMIG_CATALOG", "env_cat")
    assert load_config(TABLES_YAML).catalog == "env_cat"


def test_bad_load_type(tmp_path):
    data = _raw()
    data["tables"]["orders"]["load_type"] = "append_maybe"
    with pytest.raises(ConfigError, match="load_type"):
        load_config(_write(tmp_path, data), catalog="c")


def test_pk_must_be_a_column(tmp_path):
    data = _raw()
    data["tables"]["orders"]["primary_key"] = ["nope"]
    with pytest.raises(ConfigError, match="primary_key"):
        load_config(_write(tmp_path, data), catalog="c")


def test_pk_must_be_not_nullable(tmp_path):
    data = _raw()
    data["tables"]["orders"]["columns"][0]["nullable"] = True
    with pytest.raises(ConfigError, match="nullable"):
        load_config(_write(tmp_path, data), catalog="c")


def test_ordering_column_must_exist(tmp_path):
    data = _raw()
    data["tables"]["customers"]["ordering_column"] = "nope"
    with pytest.raises(ConfigError, match="ordering_column"):
        load_config(_write(tmp_path, data), catalog="c")


def test_unsupported_type(tmp_path):
    data = _raw()
    data["tables"]["customers"]["columns"][1]["type"] = "MAP<STRING,STRING>"
    with pytest.raises(ConfigError, match="type"):
        load_config(_write(tmp_path, data), catalog="c")


def test_fk_to_unknown_table(tmp_path):
    data = _raw()
    data["tables"]["orders"]["foreign_keys"][0]["references"]["table"] = "ghosts"
    with pytest.raises(ConfigError, match="ghosts"):
        load_config(_write(tmp_path, data), catalog="c")


def test_load_order_must_cover_all_tables(tmp_path):
    data = _raw()
    data["load_order"] = ["customers", "orders"]
    with pytest.raises(ConfigError, match="load_order"):
        load_config(_write(tmp_path, data), catalog="c")


def test_fk_parent_loaded_before_child(tmp_path):
    data = _raw()
    data["load_order"] = ["orders", "customers", "products"]
    with pytest.raises(ConfigError, match="before"):
        load_config(_write(tmp_path, data), catalog="c")


def test_quote_ident_escapes_backticks():
    assert quote_ident("a") == "`a`"
    assert quote_ident("we`ird") == "`we``ird`"


def test_sql_string_escapes():
    assert sql_string("it's") == "'it\\'s'"
    assert sql_string("a\\b") == "'a\\\\b'"


def test_parse_load_date():
    assert parse_load_date("2026-01-02") == date(2026, 1, 2)
    with pytest.raises(ConfigError):
        parse_load_date("2026-13-01")
    with pytest.raises(ConfigError):
        parse_load_date("01/02/2026")


def test_decimal_precision():
    assert decimal_precision("DECIMAL(10,2)") == (10, 2)
    assert decimal_precision("STRING") is None


def test_spec_dataclasses():
    cfg = load_config(TABLES_YAML, catalog="c")
    orders = cfg.tables["orders"]
    assert isinstance(orders, TableSpec)
    assert isinstance(orders.columns[0], Column)
    fk = orders.foreign_keys[1]
    assert isinstance(fk, ForeignKey) and fk.label == "product_id->products.product_id"
    with pytest.raises(KeyError):
        orders.column("nope")
