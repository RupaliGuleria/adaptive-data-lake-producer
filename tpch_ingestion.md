# TPC-H Ingestion

> Documents how the TPC-H benchmark dataset (`orders`, `lineitem`) was onboarded onto the
> existing banking pipeline. For the base pipeline (Kafka → ingestion → intelligence →
> Parquet → MinIO → query) see `flow.md`. This file only covers what's specific to TPC-H.

---

## 1. Where the data comes from

TPC-H isn't a file you download — it's a synthetic dataset generated on demand. Data was
generated locally using DuckDB's built-in `tpch` extension (no external dbgen toolchain
needed):

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL tpch")
con.execute("LOAD tpch")
con.execute("CALL dbgen(sf=0.01)")   # scale factor 0.01 = small test size

con.execute("COPY orders TO 'producers/cloud/data/tpch/orders.csv' (HEADER, DELIMITER ',')")
con.execute("""
  COPY (
    SELECT l_orderkey || '-' || l_linenumber AS line_item_id, *
    FROM lineitem
  ) TO 'producers/cloud/data/tpch/lineitem.csv' (HEADER, DELIMITER ',')
""")
```

| File | Rows | Notes |
|---|---|---|
| `producers/cloud/data/tpch/orders.csv` | 15,000 | One row per order; `o_orderkey` is a natural unique key |
| `producers/cloud/data/tpch/lineitem.csv` | 60,175 | One row per order line; real key is composite (`l_orderkey` + `l_linenumber`), so a `line_item_id` column (`"{orderkey}-{linenumber}"`) was precomputed at generation time so the pipeline sees a single unique ID like every other dataset |

Regenerate at a larger scale by changing `sf=` (e.g. `sf=1` → 1.5M orders, 6M lineitem rows).

---

## 2. Schema — one JSON file per table

The ingestion app already auto-loads every file under
`ingestion/src/main/resources/schemas/*.json` at startup and registers it by `schema_id`.
Two files were added there, same format as the existing `banking_transaction_v1.json`:

**`tpch_orders_v1.json`**

| Field | Type | Required |
|---|---|---|
| `o_orderkey` | number | yes |
| `o_custkey` | number | yes |
| `o_orderstatus` | string | no |
| `o_totalprice` | number | yes |
| `o_orderdate` | string | no |
| `o_orderpriority` | string | no |
| `o_clerk` | string | no |
| `o_shippriority` | number | no |
| `o_comment` | string | no |

**`tpch_lineitem_v1.json`**

| Field | Type | Required |
|---|---|---|
| `line_item_id` | string | yes *(synthetic composite key, see §1)* |
| `l_orderkey` | number | yes |
| `l_partkey` | number | no |
| `l_suppkey` | number | no |
| `l_linenumber` | number | yes |
| `l_quantity` | number | yes |
| `l_extendedprice` | number | no |
| `l_discount` | number | no |
| `l_tax` | number | no |
| `l_returnflag` | string | no |
| `l_linestatus` | string | no |
| `l_shipdate` / `l_commitdate` / `l_receiptdate` | string | no |
| `l_shipinstruct` | string | no |
| `l_shipmode` | string | no |
| `l_comment` | string | no |

Any column present in the payload but not listed here is flagged as `NEW_FIELD` schema
drift (non-breaking); any required field missing is `MISSING_FIELD` drift (breaking →
event rejected).

---

## 3. Quality rules — scoped per schema_id

**This was the one real blocker.** The quality engine used to run *every* registered rule
against *every* event regardless of dataset. The three original CRITICAL rules checked for
banking-only fields (`transaction_id`, `transaction_date`, `amount`) — so every TPC-H row
would have failed those and been rejected outright, independent of anything TPC-H-specific.

Fix: `QualityRule` now declares which `schema_id` it applies to
(`appliesTo(schemaId)`), and `QualityEngine` filters to only the rules matching the
event's schema before scoring it. Banking rules never run on TPC-H data and vice versa.

Rules registered per dataset (`ingestion/.../quality/QualityRulesConfig.java`):

| Dataset | Rule | Field | Severity |
|---|---|---|---|
| `banking_transaction_v1` | not-null | `transaction_id` | CRITICAL |
| `banking_transaction_v1` | not-null | `transaction_date` | CRITICAL |
| `banking_transaction_v1` | not-null | `amount` | CRITICAL |
| `banking_transaction_v1` | positive | `amount` | HIGH |
| `banking_transaction_v1` | date format, status enum, balance, credit score | *(pre-existing)* | HIGH/MEDIUM/LOW |
| `tpch_orders_v1` | not-null | `o_orderkey` | CRITICAL |
| `tpch_orders_v1` | not-null | `o_totalprice` | CRITICAL |
| `tpch_orders_v1` | positive | `o_totalprice` | HIGH |
| `tpch_lineitem_v1` | not-null | `line_item_id` | CRITICAL |
| `tpch_lineitem_v1` | not-null | `l_orderkey` | CRITICAL |
| `tpch_lineitem_v1` | positive | `l_quantity` | HIGH |

The old single-use `PositiveAmountRule` was generalized into a reusable
`PositiveNumberRule(field, severity, schemaId)`, following the same parameterized-bean
pattern the codebase already used for `NotNullRule`.

---

## 4. Control doc — same mechanism, per-dataset ID field

No new control-doc logic was needed — `BatchCoordinator` was always schema-agnostic. It
just needed to know which payload field is the row's unique ID. That was hardcoded in the
Python producer; it's now a config value:

| Dataset | `SCHEMA_ID` | `ID_FIELD` (trade_id / idempotency key) | `EVENT_TYPE` |
|---|---|---|---|
| Banking | `banking_transaction_v1` | `transaction_id` | `banking_transaction` |
| TPC-H orders | `tpch_orders_v1` | `o_orderkey` | `tpch_order` |
| TPC-H lineitem | `tpch_lineitem_v1` | `line_item_id` | `tpch_lineitem` |

Run each dataset with:

```bash
cd producers/cloud
CSV_FILE_PATH="data/tpch/orders.csv" SCHEMA_ID="tpch_orders_v1" \
  ID_FIELD="o_orderkey" EVENT_TYPE="tpch_order" python -m banking_producer

CSV_FILE_PATH="data/tpch/lineitem.csv" SCHEMA_ID="tpch_lineitem_v1" \
  ID_FIELD="line_item_id" EVENT_TYPE="tpch_lineitem" python -m banking_producer
```

The producer still chunks rows into groups of `PRODUCER_BATCH_SIZE` (default 500), sends one
`TradeDoc` control doc per chunk to `banking-control` with the exact set of `ID_FIELD`
values expected, then streams the chunk's events to `banking-transactions`. `BatchCoordinator`
still requires `actualCount == expectedCount` **and** exact ID-set match for `SUCCESS`,
regardless of which dataset it is.

---

## 5. NO_CONTROL_DOC retry fix (found while testing TPC-H at scale)

TPC-H's row counts meant far more control-doc chunks than any prior test (30 groups for
orders, 121 for lineitem, vs. 1 for every earlier banking smoke test). That surfaced a race:
`ControlDocListener` and `EventBatchListener` poll two Kafka topics independently with no
ordering guarantee, so an event could arrive and get checked against `BatchCoordinator`
*before* its own control doc had been registered — a few hundred milliseconds too early.

Previously this went straight to permanent DLQ (`NO_CONTROL_DOC`). Fixed in
`BatchProcessor.java` to retry with the existing backoff (2s → 4s → 8s, max 3 attempts)
before giving up to DLQ, same as any other transient error:

| Run | Events | DLQ before fix | DLQ after fix |
|---|---|---|---|
| TPC-H orders (30 groups) | 15,000 | ~2,084 (14%) | *(fix applied after this run — not re-tested)* |
| TPC-H lineitem (121 groups) | 60,175 | ~32,949 (55%) | **0** |

---

## 6. Parquet file rollover — count/time based, not a byte-size cap

**There is no byte-size limit configured for Parquet files in this pipeline.** A file rolls
over (flushes to MinIO) on whichever comes first, per `ingestion/src/main/resources/application.yml`:

```yaml
ingestion:
  intelligence:
    parquet:
      max-events-per-file: 10000     # event-count threshold
      flush-interval-ms: 60000       # 60s time threshold
```

Each partition (`year/month/day/hour/source`) buffers events in memory
(`ParquetWriteBuffer`) until either 10,000 events accumulate or 60 seconds pass, whichever
happens first, then writes one Parquet file (Snappy-compressed, dictionary-encoded) to a
local temp file and uploads it to MinIO with a single `PutObject` call — so a partially
written file is never visible in the bucket.

Observed file sizes from the TPC-H test runs (for reference — actual size depends on row
width and how compressible the data is, not just row count):

| File | Events | Size on disk (compressed) | Trigger |
|---|---|---|---|
| `orders` part-1 | 10,000 | 1,551,827 bytes (~1.5 MB) | event-count threshold |
| `orders` part-2 | 2,000 | 317,722 bytes (~310 KB) | 60s timer (partial buffer) |
| `lineitem` part-1 | 8,500 | 1,484,779 bytes (~1.4 MB) | 60s timer |
| `lineitem` part-2 | 10,000 | 1,748,135 bytes (~1.7 MB) | event-count threshold |
| Banking (earlier test) | 500 | 125,371 bytes (~122 KB) | 60s timer (small batch) |

The largest file seen in testing was **~1.7 MB at the 10,000-event ceiling** — that ceiling,
not a byte size, is what actually bounds file size today. If file size itself needs to be
capped directly (e.g. for query engine tuning), `max-events-per-file` would need to become
a size estimate instead of a raw count — not implemented.

---

## 7. Verifying it worked

Query either dataset back out through the same REST API used for banking data, filtering by
`schema_id` since all datasets currently share the same `data/` and `quarantine/` prefixes
partitioned by ingestion time, not by dataset:

```bash
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"prefix":"data","year":2026,"month":8,"day":16,"additionalWhere":"schema_id = '\''tpch_orders_v1'\''"}'
```

Test results (scale factor 0.01, after the retry fix):

| Dataset | Rows sent | Rows ingested | Schema drift | Avg quality score |
|---|---|---|---|---|
| TPC-H orders | 15,000 | 12,898 *(pre-fix run)* | 0% | 1.00 |
| TPC-H lineitem | 60,175 | 60,175 *(post-fix run)* | 0% | 1.00 |

0% schema drift confirms the schema files in §2 accurately describe the generated data;
1.00 avg quality score confirms the rules in §3 aren't firing false positives against
TPC-H's actual value ranges.
