"""Shared fixtures: a local Spark + Delta session and a config pointed at temp dirs.

Locally the Unity Catalog catalog is replaced by Spark's session catalog
(`spark_catalog`); everything else (schemas, Delta, MERGE) is the real engine.
"""
import os
import shutil
import time
from pathlib import Path

import pytest

from src.config import load_config

REPO = Path(__file__).resolve().parent.parent
TABLES_YAML = REPO / "design" / "tables.yaml"
EXPECTED_YAML = REPO / "design" / "expected.yaml"
LOCAL_CATALOG = "spark_catalog"
DAY1 = "2026-01-01"
DAY2 = "2026-01-02"

# PySpark converts collected timestamps to the Python process's local zone;
# pin it to the Spark session zone so naive datetimes compare as written.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dbmig-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def landing(tmp_path):
    """Empty landing root; tests drop CSVs under <landing>/<table>/<load_date>/."""
    root = tmp_path / "landing"
    root.mkdir()
    return root


@pytest.fixture
def cfg(landing):
    return load_config(
        TABLES_YAML,
        catalog=LOCAL_CATALOG,
        source_path=str(landing) + "/${table}/${load_date}/",
    )


@pytest.fixture
def clean(spark, cfg):
    """Drop every schema the pipeline writes to, so each test starts empty."""
    for schema in cfg.schemas():
        spark.sql(f"DROP SCHEMA IF EXISTS {cfg.schema_fqn(schema)} CASCADE")
    yield
    for schema in cfg.schemas():
        spark.sql(f"DROP SCHEMA IF EXISTS {cfg.schema_fqn(schema)} CASCADE")


def write_csv(landing: Path, table: str, load_date: str, text: str, name: str | None = None) -> Path:
    """Write a CSV fixture where the config expects it; returns the file path."""
    folder = landing / table / load_date
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (name or f"{table}_{load_date.replace('-', '')}.csv")
    path.write_text(text)
    return path


def reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
