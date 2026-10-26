"""Generic CSV -> Delta loader. Every table-specific decision comes from Config
(design/tables.yaml); nothing here knows any table by name.

Per table and load_date:
    read_source -> validate_rows -> dedupe -> with_metadata
    -> write_snapshot | write_merge -> write_rejects -> FK check -> write_audit
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src import validate
from src.config import Config, TableSpec, decimal_precision, parse_load_date, quote_ident, sql_string

CORRUPT_COL = "_corrupt_record"

REJECT_COLUMNS = [
    ("table_name", "STRING"),
    ("load_date", "DATE"),
    ("source_file", "STRING"),
    ("source_row", "BIGINT"),
    ("reason", "STRING"),
    ("raw_record", "STRING"),
    ("rejected_at", "TIMESTAMP"),
]

AUDIT_COLUMNS = [
    ("run_id", "STRING"),
    ("table_name", "STRING"),
    ("load_date", "DATE"),
    ("load_type", "STRING"),
    ("status", "STRING"),
    ("rows_read", "BIGINT"),
    ("rows_rejected", "BIGINT"),
    ("duplicates_removed", "BIGINT"),
    ("rows_inserted", "BIGINT"),
    ("rows_updated", "BIGINT"),
    ("rows_stale", "BIGINT"),
    ("fk_violations", "BIGINT"),
    ("warnings", "STRING"),
    ("error", "STRING"),
    ("started_at", "TIMESTAMP"),
    ("finished_at", "TIMESTAMP"),
]


class SourceError(RuntimeError):
    """The landing files for a table are missing or don't match the design."""


# --- DDL -------------------------------------------------------------------

def _create_table_sql(fqn: str, columns: list[tuple[str, str, bool]]) -> str:
    body = ",\n".join(f"  {quote_ident(n)} {t}{'' if nullable else ' NOT NULL'}" for n, t, nullable in columns)
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n{body}\n) USING DELTA"


def target_columns(cfg: Config, table: TableSpec) -> list[str]:
    return table.column_names + [c.name for c in cfg.metadata_columns]


def table_ddl(cfg: Config, table: TableSpec) -> str:
    cols = [(c.name, c.type, c.nullable) for c in table.columns + cfg.metadata_columns]
    return _create_table_sql(cfg.table_fqn(table.name), cols)


def rejects_ddl(cfg: Config) -> str:
    return _create_table_sql(cfg.rejects_fqn, [(n, t, True) for n, t in REJECT_COLUMNS])


def audit_ddl(cfg: Config) -> str:
    return _create_table_sql(cfg.audit_fqn, [(n, t, True) for n, t in AUDIT_COLUMNS])


def ensure_objects(spark: SparkSession, cfg: Config) -> None:
    """Create schemas and tables if missing. The catalog itself must already exist."""
    for schema in cfg.schemas():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn(schema)}")
    for name in cfg.load_order:
        spark.sql(table_ddl(cfg, cfg.tables[name]))
    spark.sql(rejects_ddl(cfg))
    spark.sql(audit_ddl(cfg))


# --- read & validate -------------------------------------------------------

def check_header(table: TableSpec, header: list[str], strict: bool) -> None:
    dupes = sorted({h for h in header if header.count(h) > 1})
    if dupes:
        raise SourceError(f"{table.name}: duplicate header column(s) {dupes}")
    missing = [c for c in table.column_names if c not in header]
    if missing:
        raise SourceError(f"{table.name}: header is missing column(s) {missing}")
    extra = [h for h in header if h not in table.column_names]
    if extra and strict:
        raise SourceError(f"{table.name}: header has unexpected column(s) {extra}")


def _reader_options(cfg: Config) -> dict:
    opts = {k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in cfg.csv_options.items()}
    opts.setdefault("header", "true")
    return opts


def read_source(spark: SparkSession, cfg: Config, table: TableSpec, load_date: str) -> DataFrame:
    """All CSVs for the table/date as STRING columns plus _source_file,
    _source_row (1-based within each file) and _corrupt_record."""
    path = cfg.source_glob(table.name, load_date)
    opts = _reader_options(cfg)
    try:
        header = spark.read.options(**opts).csv(path).columns
    except Exception as e:  # AnalysisException(PATH_NOT_FOUND) in classic and Connect
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e):
            raise SourceError(f"{table.name}: no files match {path}") from e
        raise
    check_header(table, header, cfg.strict_header)

    schema = StructType([StructField(h, StringType()) for h in header] + [StructField(CORRUPT_COL, StringType())])
    df = (
        spark.read.options(**opts)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .option("enforceSchema", "false")  # every file's header must match by name
        .schema(schema)
        .csv(path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_block_start").alias("_block"),
            F.monotonically_increasing_id().alias("_seq"),
        )
    )
    in_file = Window.partitionBy("_source_file").orderBy("_block", "_seq")
    return df.withColumn("_source_row", F.row_number().over(in_file).cast("bigint")).drop("_block", "_seq")


def cast_sql(name: str, col_type: str, csv_options: dict) -> str:
    col = quote_ident(name)
    if col_type == "STRING":
        return col
    if col_type == "TIMESTAMP":
        return f"try_to_timestamp({col}, {sql_string(csv_options.get('timestampFormat', 'yyyy-MM-dd HH:mm:ss'))})"
    if col_type == "DATE":
        return f"CAST(try_to_timestamp({col}, {sql_string(csv_options.get('dateFormat', 'yyyy-MM-dd'))}) AS DATE)"
    return f"try_cast({col} AS {col_type})"


def format_check_sql(name: str, col_type: str) -> str | None:
    """Extra strictness on top of try_cast, which would silently round decimals,
    truncate '1.5' to 1 (non-ANSI) or accept 'yes' as a boolean."""
    col = quote_ident(name)
    precision = decimal_precision(col_type)
    if precision:
        p, s = precision
        frac = f"(\\\\.[0-9]{{1,{s}}})?" if s else ""
        return f"{col} RLIKE '^[+-]?[0-9]{{1,{p - s}}}{frac}$'"
    if col_type in ("INT", "BIGINT"):
        return f"{col} RLIKE '^[+-]?[0-9]+$'"
    if col_type == "BOOLEAN":
        return f"lower({col}) IN ('true', 'false')"
    return None


def validate_rows(raw: DataFrame, cfg: Config, table: TableSpec) -> tuple[DataFrame, DataFrame]:
    """Split raw rows into (typed good rows, rejects with reason + raw_record)."""
    failures = [F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("malformed"))]
    typed = []
    for c in table.columns:
        value = F.expr(cast_sql(c.name, c.type, cfg.csv_options))
        raw_value = F.col(quote_ident(c.name))
        fmt = format_check_sql(c.name, c.type)
        bad_cast = raw_value.isNotNull() & (value.isNull() | (~F.expr(fmt) if fmt else F.lit(False)))
        failures.append(F.when(bad_cast, F.lit(f"cast:{c.name}")))
        if not c.nullable:
            failures.append(F.when(raw_value.isNull(), F.lit(f"not_null:{c.name}")))
        if c.allowed_values:
            failures.append(F.when(~bad_cast & ~value.isin(*c.allowed_values), F.lit(f"allowed_values:{c.name}")))
        typed.append(value.alias(c.name))

    raw_json = F.coalesce(F.col(CORRUPT_COL), F.to_json(F.struct(*[quote_ident(n) for n in table.column_names])))
    checked = raw.withColumn("_reason", F.concat_ws(";", *failures))
    good = checked.filter(F.col("_reason") == "").select(*typed, "_source_file", "_source_row")
    bad = checked.filter(F.col("_reason") != "").select(
        F.col("_source_file").alias("source_file"),
        F.col("_source_row").alias("source_row"),
        F.col("_reason").alias("reason"),
        raw_json.alias("raw_record"),
    )
    return good, bad


def dedupe(df: DataFrame, cfg: Config, table: TableSpec) -> DataFrame:
    """One row per PK: latest ordering_column, ties broken by defaults.dedupe_tiebreak."""
    order = [F.col(table.ordering_column).desc()]
    order += [F.col(c).desc() if d == "desc" else F.col(c).asc() for c, d in cfg.dedupe_tiebreak]
    w = Window.partitionBy(*table.primary_key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def with_metadata(df: DataFrame, load_date: str, ingested_at: datetime) -> DataFrame:
    return df.withColumn("_load_date", F.lit(parse_load_date(load_date))).withColumn(
        "_ingested_at", F.lit(ingested_at).cast("timestamp"))


# --- writers ---------------------------------------------------------------

def merge_sql(cfg: Config, table: TableSpec, source_view: str) -> str:
    on = " AND ".join(f"t.{quote_ident(k)} = s.{quote_ident(k)}" for k in table.primary_key)
    ordering = quote_ident(table.ordering_column)
    return (
        f"MERGE INTO {cfg.table_fqn(table.name)} AS t USING {quote_ident(source_view)} AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED AND s.{ordering} > t.{ordering} THEN UPDATE SET *\n"
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def snapshot_sql(cfg: Config, table: TableSpec, source_view: str) -> str:
    cols = ", ".join(quote_ident(c) for c in target_columns(cfg, table))
    return f"INSERT OVERWRITE TABLE {cfg.table_fqn(table.name)} SELECT {cols} FROM {quote_ident(source_view)}"


def _run_from_view(spark: SparkSession, cfg: Config, table: TableSpec, df: DataFrame, build_sql) -> list:
    view = f"_ingest_src_{table.name}"
    df.select(*target_columns(cfg, table)).createOrReplaceTempView(view)
    try:
        return spark.sql(build_sql(cfg, table, view)).collect()
    finally:
        spark.catalog.dropTempView(view)


def write_snapshot(spark: SparkSession, cfg: Config, table: TableSpec, df: DataFrame) -> dict:
    n = df.count()
    _run_from_view(spark, cfg, table, df, snapshot_sql)
    return {"rows_inserted": n}


def write_merge(spark: SparkSession, cfg: Config, table: TableSpec, df: DataFrame) -> dict:
    metrics = _run_from_view(spark, cfg, table, df, merge_sql)[0]
    return {"rows_inserted": int(metrics["num_inserted_rows"]), "rows_updated": int(metrics["num_updated_rows"])}


def write_rejects(spark: SparkSession, cfg: Config, table: TableSpec, load_date: str, bad: DataFrame) -> int:
    """Replace this table+load_date's rejects with `bad`; returns the row count."""
    d = parse_load_date(load_date)
    out = bad.select(
        F.lit(table.name).alias("table_name"),
        F.lit(d).alias("load_date"),
        "source_file", "source_row", "reason", "raw_record",
        F.current_timestamp().alias("rejected_at"),
    )
    where = f"table_name = {sql_string(table.name)} AND load_date = DATE'{d.isoformat()}'"
    out.write.format("delta").mode("overwrite").option("replaceWhere", where).saveAsTable(cfg.rejects_fqn)
    return bad.count()


def new_audit_record(run_id: str, table: TableSpec, load_date: str, started_at: datetime) -> dict:
    rec = {name: None for name, _ in AUDIT_COLUMNS}
    rec.update(run_id=run_id, table_name=table.name, load_date=parse_load_date(load_date),
               load_type=table.load_type, status="RUNNING", started_at=started_at)
    return rec


def write_audit(spark: SparkSession, cfg: Config, record: dict) -> None:
    schema = ", ".join(f"{n} {t}" for n, t in AUDIT_COLUMNS)
    row = tuple(record.get(n) for n, _ in AUDIT_COLUMNS)
    spark.createDataFrame([row], schema).write.format("delta").mode("append").saveAsTable(cfg.audit_fqn)


# --- orchestration ---------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_table(spark: SparkSession, cfg: Config, table: TableSpec, load_date: str, run_id: str) -> dict:
    """Load one table for one load_date and append its load_log row.
    On failure the FAILED row is logged and the exception re-raised."""
    rec = new_audit_record(run_id, table, load_date, _now())
    try:
        raw = read_source(spark, cfg, table, load_date)
        good, bad = validate_rows(raw, cfg, table)
        deduped = dedupe(good, cfg, table)
        n_read, n_bad, n_good, n_final = raw.count(), bad.count(), good.count(), deduped.count()
        final = with_metadata(deduped, load_date, rec["started_at"])

        if table.load_type == "snapshot_overwrite":
            rec.update(write_snapshot(spark, cfg, table, final))
        else:
            rec.update(write_merge(spark, cfg, table, final))
            rec["rows_stale"] = n_final - rec["rows_inserted"] - rec["rows_updated"]
        write_rejects(spark, cfg, table, load_date, bad)

        if table.foreign_keys:
            fks = validate.fk_violations(spark, cfg, table)
            rec["fk_violations"] = sum(fks.values())
            rec["warnings"] = "; ".join(f"fk:{label}={n}" for label, n in fks.items() if n) or None
        rec.update(status="SUCCESS", rows_read=n_read, rows_rejected=n_bad, duplicates_removed=n_good - n_final)
    except Exception as e:
        rec.update(status="FAILED", error=f"{type(e).__name__}: {e}"[:4000], finished_at=_now())
        write_audit(spark, cfg, rec)
        raise
    rec["finished_at"] = _now()
    write_audit(spark, cfg, rec)
    return rec


def run_batch(spark: SparkSession, cfg: Config, load_date: str, tables: list[str] | None = None) -> list[dict]:
    """Load every table (or `tables`) for load_date in cfg.load_order; stops at the first failure."""
    parse_load_date(load_date)
    ensure_objects(spark, cfg)
    run_id = str(uuid.uuid4())
    names = [n for n in cfg.load_order if tables is None or n in tables]
    return [load_table(spark, cfg, cfg.tables[n], load_date, run_id) for n in names]
