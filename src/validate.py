"""Acceptance checks from design/test_spec.md.

structural_checks: generic, derived from tables.yaml (S1-S5).
scenario_checks:   data-driven expectations from design/expected.yaml (C*, D*, M*, P*, R*, F*).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Config, TableSpec, parse_load_date

LOAD_LOG_METRICS = ("rows_read", "rows_rejected", "duplicates_removed", "rows_inserted", "rows_updated",
                    "rows_stale", "fk_violations")


@dataclass(frozen=True)
class CheckResult:
    id: str
    description: str
    passed: bool
    expected: str | None
    actual: str | None


class ValidationError(AssertionError):
    pass


def normalize(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _check(check_id: str, description: str, expected, actual) -> CheckResult:
    e, a = normalize(expected), normalize(actual)
    return CheckResult(check_id, description, e == a, e, a)


def table_exists(spark: SparkSession, fqn: str) -> bool:
    try:
        spark.table(fqn).schema
        return True
    except Exception:
        return False


def fk_violations(spark: SparkSession, cfg: Config, table: TableSpec) -> dict[str, int]:
    """{fk label: number of child rows with no matching parent row}."""
    child = spark.table(cfg.table_fqn(table.name))
    out = {}
    for fk in table.foreign_keys:
        parent = spark.table(cfg.table_fqn(fk.ref_table)).select(
            *[F.col(r).alias(c) for r, c in zip(fk.ref_columns, fk.columns)]).distinct()
        out[fk.label] = child.join(parent, on=fk.columns, how="left_anti").count()
    return out


def structural_checks(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    results = []
    for name in cfg.load_order:
        table = cfg.tables[name]
        fqn = cfg.table_fqn(name)
        if not table_exists(spark, fqn):
            results.append(CheckResult("S1", f"{name}: table exists and is Delta", False, "delta", "missing"))
            continue
        fmt = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]["format"]
        results.append(_check("S1", f"{name}: table exists and is Delta", "delta", fmt))

        df = spark.table(fqn)
        expected_schema = [(c.name, c.type.lower()) for c in table.columns + cfg.metadata_columns]
        actual_schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        results.append(_check("S2", f"{name}: columns and types match tables.yaml", expected_schema, actual_schema))

        required = [c.name for c in table.columns if not c.nullable]
        nulls = df.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required]).collect()[0]
        null_cols = {c: nulls[c] for c in required if nulls[c]}
        results.append(_check("S3", f"{name}: no NULLs in nullable:false columns", {}, null_cols))

        dupes = df.groupBy(*table.primary_key).count().filter("count > 1").count()
        results.append(_check("S4", f"{name}: primary key {table.primary_key} unique",
                              "0 duplicate key(s)", f"{dupes} duplicate key(s)"))

    for fqn in (cfg.rejects_fqn, cfg.audit_fqn):
        results.append(_check("S5", f"{fqn} exists", True, table_exists(spark, fqn)))
    return results


def load_expected(path: str | Path, load_date: str) -> dict:
    """Expectations for one load_date from design/expected.yaml (KeyError if absent)."""
    return yaml.safe_load(Path(path).read_text())[load_date]


def _key_filter(key: dict) -> SparkColumn:
    cond = F.lit(True)
    for col, value in key.items():
        cond = cond & (F.col(col) == F.lit(value))
    return cond


def _first_success_runs(spark: SparkSession, cfg: Config, d: date) -> dict[str, dict]:
    rows = (spark.table(cfg.audit_fqn)
            .filter((F.col("load_date") == F.lit(d)) & (F.col("status") == "SUCCESS"))
            .orderBy("finished_at").collect())
    first = {}
    for r in rows:
        first.setdefault(r["table_name"], r.asDict())
    return first


def scenario_checks(spark: SparkSession, cfg: Config, load_date: str, expected: dict) -> list[CheckResult]:
    d = parse_load_date(load_date)
    table = lambda name: spark.table(cfg.table_fqn(name))  # noqa: E731
    results = []

    for c in expected.get("counts", []):
        results.append(_check(c["id"], f"{c['table']}: row count", c["expected"], table(c["table"]).count()))

    if "rejected" in expected:
        n = spark.table(cfg.rejects_fqn).filter(F.col("load_date") == F.lit(d)).count()
        results.append(_check("C4", f"rejected rows for {load_date}", expected["rejected"], n))

    runs = _first_success_runs(spark, cfg, d) if expected.get("load_log") else {}
    for name, metrics in expected.get("load_log", {}).items():
        run = runs.get(name)
        actual = {m: run[m] for m in metrics} if run else "no SUCCESS run"
        results.append(_check("C6", f"{name}: load_log metrics (first SUCCESS run)", dict(metrics), actual))

    for s in expected.get("sums", []):
        total = table(s["table"]).agg(F.sum(s["column"])).collect()[0][0]
        results.append(_check(s["id"], f"{s['table']}: SUM({s['column']})", s["expected"], total))

    for v in expected.get("values", []):
        # Cast in Spark (session time zone) rather than relying on the driver's local zone.
        rows = (table(v["table"]).filter(_key_filter(v["key"]))
                .select(*[F.col(c).cast("string").alias(c) for c in v["expect"]]).collect())
        for col, want in v["expect"].items():
            desc = f"{v['table']} {v['key']}: {col}"
            if len(rows) != 1:
                results.append(CheckResult(v["id"], desc, False, normalize(want), f"{len(rows)} rows"))
            else:
                results.append(_check(v["id"], desc, want, rows[0][col]))

    for a in expected.get("absent", []):
        n = table(a["table"]).filter(_key_filter(a["key"])).count()
        results.append(_check(a["id"], f"{a['table']} {a['key']}: absent (rows)", 0, n))

    for k in expected.get("key_set", []):
        pk = cfg.tables[k["table"]].primary_key
        got = sorted(normalize(r[0]) for r in table(k["table"]).select(*pk).collect())
        results.append(_check(k["id"], f"{k['table']}: exact key set", sorted(map(normalize, k["keys"])), got))

    for r in expected.get("rejects", []):
        n = spark.table(cfg.rejects_fqn).filter(
            (F.col("table_name") == r["table"]) & (F.col("load_date") == F.lit(d))
            & (F.col("source_row") == r["source_row"]) & (F.col("reason") == r["reason"])).count()
        results.append(_check(r["id"], f"{r['table']} reject row {r['source_row']}: {r['reason']} (rows)", 1, n))

    violations: dict[str, dict] = {}
    for f in expected.get("fk", []):
        if f["table"] not in violations:
            violations[f["table"]] = fk_violations(spark, cfg, cfg.tables[f["table"]])
        results.append(_check(f["id"], f"{f['table']}: rows violating {f['fk']}",
                              f["expected"], violations[f["table"]].get(f["fk"], "unknown fk")))
    return results


def _latest_run_check(spark: SparkSession, cfg: Config, d: date) -> CheckResult:
    log = spark.table(cfg.audit_fqn).filter(F.col("load_date") == F.lit(d))
    latest = log.orderBy(F.col("started_at").desc()).select("run_id").limit(1).collect()
    n = log.filter((F.col("run_id") == latest[0][0]) & (F.col("status") == "SUCCESS")).count() if latest else 0
    return _check("C5", f"latest run for {d}: every table SUCCESS", len(cfg.tables), n)


def run_checks(spark: SparkSession, cfg: Config, load_date: str, expected: dict | None = None) -> list[CheckResult]:
    results = structural_checks(spark, cfg)
    results.append(_latest_run_check(spark, cfg, parse_load_date(load_date)))
    if expected:
        results += scenario_checks(spark, cfg, load_date, expected)
    return results


def format_results(results: list[CheckResult]) -> str:
    return "\n".join(
        f"{'PASS' if r.passed else 'FAIL'}  {r.id:<3} {r.description}  expected={r.expected} actual={r.actual}"
        for r in results)


def assert_all_passed(results: list[CheckResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        raise ValidationError(f"{len(failed)} check(s) failed:\n{format_results(failed)}")
