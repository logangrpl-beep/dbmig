# Databricks notebook source
# pyright: reportUndefinedVariable=false
# (spark, dbutils and display are notebook globals on Databricks)

# COMMAND ----------

# MAGIC %md
# MAGIC # dbmig: CSV → Delta pipeline
# MAGIC Loads every table in `design/tables.yaml` for one `load_date` from
# MAGIC `/Volumes/<catalog>/landing/raw/<table>/<load_date>/`, then runs the acceptance checks.
# MAGIC
# MAGIC Widgets: `catalog` (required), `load_date` (YYYY-MM-DD), `run_checks` (`structural` | `scenario` | `none`).
# MAGIC `scenario` also compares against `design/expected.yaml` (only meaningful for the generated test data).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("load_date", "")
dbutils.widgets.dropdown("run_checks", "structural", ["structural", "scenario", "none"])

catalog = dbutils.widgets.get("catalog").strip()
load_date = dbutils.widgets.get("load_date").strip()
run_checks = dbutils.widgets.get("run_checks")
if not catalog or not load_date:
    raise ValueError("Set the 'catalog' and 'load_date' widgets")

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src import ingest, validate
from src.config import load_config

cfg = load_config(os.path.join(repo_root, "design", "tables.yaml"), catalog=catalog)
print(f"catalog={cfg.catalog} load_date={load_date} tables={cfg.load_order}")

# COMMAND ----------

records = ingest.run_batch(spark, cfg, load_date)
display(spark.table(cfg.audit_fqn).filter(f"run_id = '{records[0]['run_id']}'").orderBy("started_at"))

# COMMAND ----------

if run_checks != "none":
    expected = None
    if run_checks == "scenario":
        expected = validate.load_expected(os.path.join(repo_root, "design", "expected.yaml"), load_date)
    results = validate.run_checks(spark, cfg, load_date, expected)
    print(validate.format_results(results))
    validate.assert_all_passed(results)
