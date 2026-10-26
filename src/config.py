"""Load and validate design/tables.yaml into typed, catalog-resolved config objects."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from string import Template

import yaml

LOAD_TYPES = ("snapshot_overwrite", "merge_upsert")
SIMPLE_TYPES = ("STRING", "INT", "BIGINT", "BOOLEAN", "DATE", "TIMESTAMP")
DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CATALOG_ENV = "DBMIG_CATALOG"


class ConfigError(ValueError):
    pass


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def parse_load_date(value: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
        raise ConfigError(f"load_date must be YYYY-MM-DD, got {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise ConfigError(f"invalid load_date {value!r}: {e}") from None


def decimal_precision(col_type: str) -> tuple[int, int] | None:
    m = DECIMAL_RE.match(col_type)
    return (int(m.group(1)), int(m.group(2))) if m else None


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    allowed_values: list | None = None
    desc: str | None = None


@dataclass(frozen=True)
class ForeignKey:
    columns: list[str]
    ref_table: str
    ref_columns: list[str]

    @property
    def label(self) -> str:
        return f"{','.join(self.columns)}->{self.ref_table}.{','.join(self.ref_columns)}"


@dataclass(frozen=True)
class TableSpec:
    name: str
    description: str
    file_pattern: str
    primary_key: list[str]
    load_type: str
    ordering_column: str
    columns: list[Column]
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


@dataclass(frozen=True)
class Config:
    catalog: str
    source_path: str
    csv_options: dict
    strict_header: bool
    target_schema: str
    metadata_columns: list[Column]
    dedupe_tiebreak: list[tuple[str, str]]
    rejects_table: str
    audit_table: str
    tables: dict[str, TableSpec]
    load_order: list[str]

    def schema_fqn(self, schema: str) -> str:
        return f"{quote_ident(self.catalog)}.{quote_ident(schema)}"

    def table_fqn(self, table: str) -> str:
        return f"{self.schema_fqn(self.target_schema)}.{quote_ident(table)}"

    def _ops_fqn(self, dotted: str) -> str:
        schema, table = dotted.split(".")
        return f"{self.schema_fqn(schema)}.{quote_ident(table)}"

    @property
    def rejects_fqn(self) -> str:
        return self._ops_fqn(self.rejects_table)

    @property
    def audit_fqn(self) -> str:
        return self._ops_fqn(self.audit_table)

    def schemas(self) -> list[str]:
        names = [self.target_schema] + [t.split(".")[0] for t in (self.rejects_table, self.audit_table)]
        return list(dict.fromkeys(names))

    def source_dir(self, table: str, load_date: str) -> str:
        path = Template(self.source_path).substitute(catalog=self.catalog, table=table, load_date=load_date)
        return path if path.endswith("/") else path + "/"

    def source_glob(self, table: str, load_date: str) -> str:
        return self.source_dir(table, load_date) + self.tables[table].file_pattern


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ConfigError(msg)


def _parse_column(raw: dict, where: str) -> Column:
    _require("name" in raw and "type" in raw, f"{where}: column needs name and type")
    col = Column(
        name=raw["name"],
        type=str(raw["type"]).upper().replace(" ", ""),
        nullable=bool(raw.get("nullable", True)),
        allowed_values=raw.get("allowed_values"),
        desc=raw.get("desc"),
    )
    _require(IDENT_RE.match(col.name) is not None, f"{where}: bad column name {col.name!r}")
    _require(col.type in SIMPLE_TYPES or decimal_precision(col.type) is not None,
             f"{where}.{col.name}: unsupported type {col.type!r}")
    return col


def _parse_table(name: str, raw: dict) -> TableSpec:
    where = f"tables.{name}"
    columns = [_parse_column(c, where) for c in raw.get("columns", [])]
    _require(bool(columns), f"{where}: no columns")
    names = [c.name for c in columns]
    _require(len(set(names)) == len(names), f"{where}: duplicate column names")
    by_name = {c.name: c for c in columns}

    pk = list(raw.get("primary_key") or [])
    _require(bool(pk) and all(k in by_name for k in pk), f"{where}: primary_key {pk} must name columns")
    _require(all(not by_name[k].nullable for k in pk), f"{where}: primary_key columns must be nullable: false")

    load_type = raw.get("load_type")
    _require(load_type in LOAD_TYPES, f"{where}: load_type {load_type!r} not in {LOAD_TYPES}")

    ordering = raw.get("ordering_column")
    _require(ordering in by_name, f"{where}: ordering_column {ordering!r} is not a column")
    _require(not by_name[ordering].nullable, f"{where}: ordering_column must be nullable: false")

    fks = []
    for fk in raw.get("foreign_keys") or []:
        ref = fk["references"]
        fks.append(ForeignKey(list(fk["columns"]), ref["table"], list(ref["columns"])))
        _require(all(c in by_name for c in fk["columns"]), f"{where}: foreign key columns {fk['columns']}")

    return TableSpec(
        name=name,
        description=raw.get("description", ""),
        file_pattern=(raw.get("source") or {}).get("file_pattern", f"{name}_*.csv"),
        primary_key=pk,
        load_type=load_type,
        ordering_column=ordering,
        columns=columns,
        foreign_keys=fks,
    )


def load_config(path: str | Path, catalog: str | None = None, source_path: str | None = None) -> Config:
    """Parse tables.yaml. `catalog` falls back to $DBMIG_CATALOG; `source_path`
    overrides defaults.source.path (a template using ${catalog}/${table}/${load_date})."""
    catalog = catalog if catalog is not None else os.environ.get(CATALOG_ENV)
    _require(bool(catalog) and IDENT_RE.match(catalog) is not None,
             f"catalog must be a plain identifier (got {catalog!r}); pass catalog= or set {CATALOG_ENV}")

    raw = yaml.safe_load(Path(path).read_text())
    defaults = raw.get("defaults", {})
    source = defaults.get("source", {})
    target = defaults.get("target", {})

    tables = {name: _parse_table(name, t) for name, t in (raw.get("tables") or {}).items()}
    _require(bool(tables), "no tables defined")

    for t in tables.values():
        for fk in t.foreign_keys:
            _require(fk.ref_table in tables, f"tables.{t.name}: foreign key references unknown table {fk.ref_table!r}")
            parent = tables[fk.ref_table]
            _require(all(c in parent.column_names for c in fk.ref_columns),
                     f"tables.{t.name}: foreign key references unknown columns {fk.ref_columns}")

    load_order = list(raw.get("load_order") or tables)
    _require(sorted(load_order) == sorted(tables), f"load_order {load_order} must list every table exactly once")
    for t in tables.values():
        for fk in t.foreign_keys:
            _require(load_order.index(fk.ref_table) < load_order.index(t.name),
                     f"load_order: {fk.ref_table} must load before {t.name}")

    return Config(
        catalog=catalog,
        source_path=source_path or source["path"],
        csv_options=dict(source.get("options", {})),
        strict_header=bool(source.get("strict_header", True)),
        target_schema=target.get("schema", "bronze"),
        metadata_columns=[_parse_column(c, "defaults.metadata_columns") for c in defaults.get("metadata_columns", [])],
        dedupe_tiebreak=[(d["column"], d.get("direction", "desc").lower()) for d in defaults.get("dedupe_tiebreak", [])],
        rejects_table=defaults.get("rejects_table", "ops.rejected_rows"),
        audit_table=defaults.get("audit_table", "ops.load_log"),
        tables=tables,
        load_order=load_order,
    )
