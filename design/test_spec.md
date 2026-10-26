# dbmig — acceptance test spec

Covers the 3 tables defined in `design/tables.yaml` (customers, products, orders),
loaded from CSV in the landing volume into Delta tables in `${catalog}.bronze`.
`${catalog}` is a parameter in every check; no check may assume `dbmig`.

Two batches are loaded in order:

| Batch | load_date  | Landing path                                             |
|-------|------------|----------------------------------------------------------|
| Day 1 | 2026-01-01 | `/Volumes/${catalog}/landing/raw/<table>/2026-01-01/`    |
| Day 2 | 2026-01-02 | `/Volumes/${catalog}/landing/raw/<table>/2026-01-02/`    |

Within each batch the load order is `customers → products → orders`.

Each fixture file below is deliberately small and contains one or more edge cases
(duplicates, stale updates, bad rows, dropped snapshot rows) so every expected count
can be derived by hand. `scripts/generate_data.py` writes them to `<out>/<table>/<load_date>/<table>_<yyyymmdd>.csv`;
expected results are in machine-readable form in `design/expected.yaml`.

---

## 1. Fixture data

### 1.1 customers (`merge_upsert`, PK `customer_id`, ordering `updated_at`)

**Day 1 — `customers_20260101.csv`** (7 data rows)

```csv
customer_id,email,first_name,last_name,country,created_at,updated_at
1,alice@ex.com,Alice,Ng,US,2025-06-01 08:00:00,2025-06-01 08:00:00
2,bob@ex.com,Bob,Lee,GB,2025-06-02 08:00:00,2025-06-02 08:00:00
3,carol@old.com,Carol,Diaz,ES,2025-06-03 08:00:00,2025-06-03 08:00:00
3,carol@new.com,Carol,Diaz,ES,2025-06-03 08:00:00,2025-12-31 10:00:00
4,dan@ex.com,Dan,Kim,KR,2025-06-04 08:00:00,2025-12-01 12:00:00
5,eve@ex.com,Eve,Roe,DE,2025-06-05 08:00:00,2025-06-05 08:00:00
,nopk@ex.com,No,Key,US,2025-06-06 08:00:00,2025-06-06 08:00:00
```

| Edge case | Rows | Expected |
|---|---|---|
| Duplicate PK, different `updated_at` | customer 3 ×2 | keep `carol@new.com` (later `updated_at`) |
| NULL PK | row 7 | rejected, reason `not_null:customer_id` |

**Day 2 — `customers_20260102.csv`** (4 data rows)

```csv
customer_id,email,first_name,last_name,country,created_at,updated_at
2,bob@new.com,Bob,Lee,GB,2025-06-02 08:00:00,2026-01-02 09:00:00
6,frank@ex.com,Frank,Moreau,FR,2026-01-02 07:00:00,2026-01-02 07:00:00
7,grace@ex.com,Grace,Rossi,IT,2026-01-02 08:00:00,2026-01-02 08:00:00
4,dan@stale.com,Dan,Kim,KR,2025-06-04 08:00:00,2025-11-01 12:00:00
```

| Edge case | Rows | Expected |
|---|---|---|
| Update of existing row | customer 2 | email → `bob@new.com` |
| New rows | customers 6, 7 | inserted |
| Stale update (`updated_at` older than target) | customer 4 | ignored; email stays `dan@ex.com` |

### 1.2 products (`snapshot_overwrite`, PK `product_id`, ordering `updated_at`)

**Day 1 — `products_20260101.csv`** (5 data rows)

```csv
product_id,name,category,unit_price,is_active,updated_at
P001,Widget,Tools,9.99,true,2025-11-01 00:00:00
P002,Gadget,Tools,19.99,true,2025-12-01 00:00:00
P002,Gadget,Tools,24.99,true,2025-12-15 00:00:00
P003,Gizmo,Toys,4.50,true,2025-11-01 00:00:00
P004,Doohickey,Misc,14.00,true,2025-11-01 00:00:00
```

| Edge case | Rows | Expected |
|---|---|---|
| Duplicate PK | P002 ×2 | keep `unit_price = 24.99` |

**Day 2 — `products_20260102.csv`** (4 data rows, full extract)

```csv
product_id,name,category,unit_price,is_active,updated_at
P001,Widget,Tools,9.99,true,2025-11-01 00:00:00
P002,Gadget,Tools,21.99,true,2026-01-02 00:00:00
P003,Gizmo,Toys,4.50,true,2025-11-01 00:00:00
P005,Thingamajig,Misc,30.00,true,2026-01-02 00:00:00
```

| Edge case | Rows | Expected |
|---|---|---|
| Price change | P002 | `unit_price = 21.99` |
| Row dropped from snapshot | P004 | absent from target |
| New product | P005 | present |

### 1.3 orders (`merge_upsert`, PK `order_id`, ordering `updated_at`)

**Day 1 — `orders_20260101.csv`** (11 data rows)

```csv
order_id,customer_id,product_id,quantity,unit_price,order_status,order_ts,updated_at
1001,1,P001,2,9.99,DELIVERED,2025-12-20 10:00:00,2025-12-28 10:00:00
1002,2,P002,1,24.99,SHIPPED,2025-12-29 10:00:00,2025-12-31 10:00:00
1003,3,P003,3,4.50,PLACED,2026-01-01 08:00:00,2026-01-01 08:00:00
1003,3,P003,3,4.50,SHIPPED,2026-01-01 08:00:00,2026-01-01 15:00:00
1004,4,P001,1,9.99,PLACED,2026-01-01 09:00:00,2026-01-01 09:00:00
1005,5,P002,4,24.99,PLACED,2026-01-01 09:30:00,2026-01-01 09:30:00
1005,5,P002,4,24.99,PLACED,2026-01-01 09:30:00,2026-01-01 09:30:00
1006,1,P003,2,4.50,PLACED,2026-01-01 10:00:00,2026-01-01 10:00:00
1007,2,P001,1,9.99,CANCELLED,2026-01-01 11:00:00,2026-01-01 12:00:00
1008,3,P004,5,14.00,PLACED,2026-01-01 13:00:00,2026-01-01 13:00:00
1099,1,P001,1,9.99,LOST,2026-01-01 14:00:00,2026-01-01 14:00:00
```

| Edge case | Rows | Expected |
|---|---|---|
| Duplicate PK, status progression | 1003 ×2 | keep `SHIPPED` |
| Exact duplicate row (tie on `updated_at`) | 1005 ×2 | one row kept (tiebreak by `_source_row`) |
| Invalid `order_status` | 1099 | rejected, reason `allowed_values:order_status` |

**Day 2 — `orders_20260102.csv`** (6 data rows)

```csv
order_id,customer_id,product_id,quantity,unit_price,order_status,order_ts,updated_at
1002,2,P002,1,24.99,DELIVERED,2025-12-29 10:00:00,2026-01-02 10:00:00
1003,3,P003,3,4.50,DELIVERED,2026-01-01 08:00:00,2026-01-02 11:00:00
1009,6,P005,1,30.00,PLACED,2026-01-02 08:00:00,2026-01-02 08:00:00
1010,7,P001,2,9.99,PLACED,2026-01-02 09:00:00,2026-01-02 09:00:00
1011,2,P005,3,30.00,PLACED,2026-01-02 10:00:00,2026-01-02 10:00:00
1012,1,P002,1,21.99,PLACED,2026-01-02 11:00:00,2026-01-02 11:00:00
```

| Edge case | Rows | Expected |
|---|---|---|
| Status updates | 1002, 1003 | both → `DELIVERED` |
| New orders referencing day-2 parents | 1009 (cust 6, P005), 1010 (cust 7) | inserted, no FK warning |
| Parent removed by snapshot | 1008 → P004 | 1 FK warning on day 2 (row is **kept**) |

---

## 2. Expected counts

### 2.1 Day 1 (after loading 2026-01-01 into an empty catalog)

| Table | Raw rows read | Rejected | Duplicates removed | Inserted | Updated | Stale ignored | **Target rows** |
|---|---|---|---|---|---|---|---|
| customers | 7  | 1 | 1 | 5 | 0 | 0 | **5** |
| products  | 5  | 0 | 1 | 4 (overwrite) | – | – | **4** |
| orders    | 11 | 1 | 2 | 8 | 0 | 0 | **8** |

- `${catalog}.ops.rejected_rows` for load_date 2026-01-01: **2** (1 customers, 1 orders)
- FK violations on orders: **0**
- `SUM(orders.quantity)` = **19**

### 2.2 Day 2 (after loading 2026-01-02 on top of day 1)

| Table | Raw rows read | Rejected | Duplicates removed | Inserted | Updated | Stale ignored | **Target rows** |
|---|---|---|---|---|---|---|---|
| customers | 4 | 0 | 0 | 2 | 1 | 1 | **7** |
| products  | 4 | 0 | 0 | 4 (overwrite) | – | – | **4** |
| orders    | 6 | 0 | 0 | 4 | 2 | 0 | **12** |

- `${catalog}.ops.rejected_rows` for load_date 2026-01-02: **0** (cumulative total still **2**)
- FK violations on orders: **1** (`order_id 1008 → product_id P004`)
- `SUM(orders.quantity)` = **26**

---

## 3. Acceptance checks

Every check runs against `${catalog}` and must pass after the stated batch.
"D1" = after day 1, "D2" = after day 2.

### 3.1 Structural (D1 and D2)

| ID | Check | Expected |
|---|---|---|
| S1 | `${catalog}.bronze.{customers,products,orders}` exist and are Delta | true |
| S2 | Column names, order and types match `tables.yaml` + the 4 metadata columns | exact match |
| S3 | `nullable: false` columns contain no NULLs | 0 NULLs |
| S4 | PK is unique: `COUNT(*) = COUNT(DISTINCT <pk>)` for each table | equal |
| S5 | `${catalog}.ops.rejected_rows` and `${catalog}.ops.load_log` exist | true |
| S6 | No object is created in a catalog other than `${catalog}` | true |

### 3.2 Counts

| ID | Check | D1 | D2 |
|---|---|---|---|
| C1 | `COUNT(*)` customers | 5 | 7 |
| C2 | `COUNT(*)` products | 4 | 4 |
| C3 | `COUNT(*)` orders | 8 | 12 |
| C4 | rejected rows for that load_date | 2 | 0 |
| C5 | `load_log` rows in the latest run for that load_date with `status = 'SUCCESS'` | 3 | 3 |
| C6 | `load_log` per-table rows_read / rejected / inserted / updated / stale, on the **first** SUCCESS run for that table+date | matches §2.1 | matches §2.2 |
| C7 | `SUM(orders.quantity)` | 19 | 26 |

### 3.3 Dedupe & ordering

| ID | Check | Expected |
|---|---|---|
| D1 | customers 3 `email` (D1, D2) | `carol@new.com` |
| D2 | products P002 `unit_price` (D1) | `24.99` |
| D3 | orders 1003 `order_status` (D1) | `SHIPPED` |
| D4 | orders 1005 row count (D1, D2) | 1 |

### 3.4 Merge semantics (D2)

| ID | Check | Expected |
|---|---|---|
| M1 | customers 2 `email` / `updated_at` | `bob@new.com` / `2026-01-02 09:00:00` |
| M2 | customers 4 `email` / `updated_at` (stale row ignored) | `dan@ex.com` / `2025-12-01 12:00:00` |
| M3 | customers 6 and 7 exist with `_load_date = 2026-01-02` | true |
| M4 | customers 1, 3, 5 `_load_date` (untouched rows keep D1 metadata) | `2026-01-01` |
| M5 | orders 1002 and 1003 `order_status` | `DELIVERED` |
| M6 | orders 1001, 1004–1008 unchanged from D1 (all columns) | true |

### 3.5 Snapshot semantics (D2)

| ID | Check | Expected |
|---|---|---|
| P1 | products contains exactly `{P001, P002, P003, P005}` | true |
| P2 | P004 absent | true |
| P3 | P002 `unit_price` | `21.99` |
| P4 | all products rows have `_load_date = 2026-01-02` | true |

### 3.6 Rejects

| ID | Check | Expected |
|---|---|---|
| R1 | D1 customers reject: `_source_row = 7`, reason `not_null:customer_id` | 1 row |
| R2 | D1 orders reject: `_source_row = 11`, reason `allowed_values:order_status` | 1 row |
| R3 | No rejected row appears in any target table (e.g. order 1099 absent) | true |

### 3.7 Referential integrity (warning only)

| ID | Check | D1 | D2 |
|---|---|---|---|
| F1 | orders with no matching customer | 0 | 0 |
| F2 | orders with no matching product | 0 | 1 (1008 → P004) |
| F3 | F2 violation recorded in `load_log` as a warning; load status still `SUCCESS` | – | true |

### 3.8 Idempotency

| ID | Check | Expected |
|---|---|---|
| I1 | Re-run day 2 for all tables → all of §3.2–§3.7 D2 results unchanged (the re-run's own log row reports 0 inserted / 0 updated, all rows stale) | true |
| I2 | Re-run day 2 → rejected rows for 2026-01-02 still 0; for 2026-01-01 still 2 (replaced, not appended) | true |
| I3 | Re-run day 1 on an empty catalog twice in a row → same as §2.1 | true |
| I4 | Re-run adds new `load_log` rows (audit history) but no target-table changes | true |

### 3.9 Configurability

| ID | Check | Expected |
|---|---|---|
| X1 | Full D1 + D2 run against a second catalog name (e.g. `dbmig_test`) passes all checks above | true |
| X2 | `grep -r "dbmig\." src/` finds no hardcoded catalog references | 0 matches |

---

## 4. Out of scope / assumptions (please confirm)

1. **Hard deletes** are only expressed through `snapshot_overwrite` (products). Customers and orders never delete rows.
2. **Out-of-order replays** (e.g. re-running day 1 after day 2) are not supported. For `snapshot_overwrite` it would revert products to the day-1 snapshot.
3. **FK violations are warnings**, not rejects, because a snapshot can legitimately remove a parent still referenced by history (order 1008 → P004).
4. **Target layer** is `${catalog}.bronze` (schemas `landing`, `bronze`, `ops` already exist in `databricks_init.sql`).
5. **One CSV per table per batch**; if several files match `file_pattern`, they're all read and the tiebreak (`_source_file DESC, _source_row DESC`) applies.
