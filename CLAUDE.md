# dbmig
Databricks migration scripts: Unity Catalog setup (catalogs, schemas, volumes).

## Environment
- Ubuntu on WSL, run from VS Code
- Use the official `databricks` CLI (auth: `databricks auth login`)
- Do NOT use dbsqlcli (unmaintained, dependency issues)

## Commands
- Run a SQL file: `./run_sql.sh <file.sql> <warehouse_id>`
- List warehouses: `databricks warehouses list`

# dbmig migration – agent rules
- Phases: design -> build -> run. Don't start a phase until the previous phase's files exist and I approve them.
- All table behaviour comes from design/tables.yaml. No per-table code.
- Every function in src/ needs a test in tests/. Run `pytest -q` and fix until green before saying done.
- Target catalog is configurable; never hardcode `dbmig`.