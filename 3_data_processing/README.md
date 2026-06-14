# Section 3: Data Processing and Transformations

This is the largest section on the exam. It covers the Medallion Architecture,
Delta Live Pipelines (Lakeflow Declarative Pipelines), DDL/DML on Delta tables,
cluster configuration for processing workloads, and PySpark transformations
and aggregations.

---

## Medallion Architecture

A data design pattern that organizes data into three progressively refined layers.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │ ──► │   SILVER    │ ──► │    GOLD     │
│ Raw, as-is  │     │ Cleaned,    │     │ Aggregated, │
│ from source │     │ validated,  │     │ business-   │
│             │     │ deduplicated│     │ level       │
└─────────────┘     └─────────────┘     └─────────────┘
```

### Bronze Layer
```
Purpose:    Raw, unprocessed copy of source data
Schema:     Matches source as closely as possible
Format:     Delta (even if source is CSV/JSON/Parquet)
Mutations:  Append-only — never update or delete
Why:        Single source of truth — if downstream logic has a bug,
            reprocess from Bronze without re-extracting from source
```

### Silver Layer
```
Purpose:    Cleaned, validated, conformed data
Operations: Deduplication, null handling, type casting,
            column renaming, schema enforcement, joins across sources
Schema:     Business entity-oriented — e.g. one table per entity
            (customers, orders, products) rather than per source system
Why:        Single queryable layer for data scientists and analysts
            who need clean but not yet aggregated data
```

### Gold Layer
```
Purpose:    Aggregated, business-level tables
Operations: groupBy, joins for reporting, window functions,
            pre-computed KPIs and metrics
Schema:     Denormalized, optimized for specific consumption patterns
            (dashboards, reports, ML feature tables)
Why:        Fast queries for BI tools — aggregation already done,
            consumers don't scan raw data
```

### Why Layer Instead of Transforming Directly

```
Without medallion layers:
  Source → one big transformation → final table
  If transformation has a bug, must re-extract from source
  No intermediate checkpoint to debug from
  Mixing concerns: parsing, cleaning, AND aggregating in one step

With medallion layers:
  Each layer has ONE responsibility
  Bronze: capture exactly what arrived
  Silver: make it correct and clean
  Gold: make it useful for a specific purpose
  Can reprocess any layer independently
  Each layer is independently testable
```

---

## Delta Live Pipelines (Lakeflow Declarative Pipelines)

DLP is the declarative framework for building medallion pipelines.
You declare what each table should contain; Databricks manages execution
order, checkpoints, retries, and monitoring.

### The Three Decorators

```python
import dlt

@dlt.table              # creates a materialized Delta table
@dlt.view                # creates a temporary view, not persisted to storage
@dlt.table(name="...")   # explicit table naming
```

### Reading From Other DLP Tables

```python
dlt.read("table_name")         # batch read — full current state
dlt.read_stream("table_name")  # streaming read — only new records
```

```python
# Bronze — streaming source via Auto Loader
@dlt.table
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/landing/orders/")
    )

# Silver — streaming read from Bronze, batch transform logic
@dlt.table
def silver_orders():
    return (dlt.read_stream("bronze_orders")
        .dropDuplicates(["order_id"])
        .filter(col("amount") > 0)
    )

# Gold — batch read from Silver, aggregation
@dlt.table
def gold_daily_revenue():
    return (dlt.read("silver_orders")
        .groupBy("order_date")
        .agg(sum("amount").alias("total_revenue"))
    )
```

### Data Quality Expectations

```python
# expect — keep record, flag violation in monitoring UI
@dlt.expect("valid_amount", "amount > 0")

# expect_or_drop — silently drop violating records, pipeline continues
@dlt.expect_or_drop("non_null_customer", "customer_id IS NOT NULL")

# expect_or_fail — stop the entire pipeline if any record violates
@dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
```

**Critical mental model:** the condition describes what GOOD data looks like.
Records that DON'T match the condition are violations.

```python
# Correct — good data has non-null amount
# violating records (null amount) get dropped
@dlt.expect_or_drop("non_null_amount", "amount IS NOT NULL")

# WRONG — this says good data HAS null amount
# drops every record where amount IS NOT NULL (i.e. everything valid)
@dlt.expect_or_drop("non_null_amount", "amount IS NULL")
```

### Multiple Expectations on One Table

```python
@dlt.table
@dlt.expect("valid_quality_range", "quality BETWEEN 0 AND 10")
@dlt.expect_or_drop("non_null_alcohol", "alcohol IS NOT NULL")
@dlt.expect_or_drop("non_null_pH", "pH IS NOT NULL")
def silver_wine_quality():
    return dlt.read_stream("bronze_wine_quality").dropDuplicates()
```

Each expectation is tracked independently in the pipeline UI — you can see
pass/fail counts per rule.

### SQL Syntax for DLP

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT * FROM STREAM read_files(
    '/mnt/landing/orders/',
    format => 'json'
)

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
AS SELECT order_date, SUM(amount) AS total_revenue
FROM LIVE.silver_orders
GROUP BY order_date
```

```
SQL                          Python equivalent
─────────────────────────────────────────────────────
STREAMING TABLE          ──► @dlt.table returning streaming DataFrame
MATERIALIZED VIEW         ──► @dlt.table returning batch DataFrame
LIVE.table_name           ──► dlt.read("table_name")
STREAM LIVE.table_name     ──► dlt.read_stream("table_name")
read_files()              ──► spark.readStream.format("cloudFiles")
```

### Pipeline Execution Modes

```
Triggered:   runs once, processes available data, then stops
             like a batch job — good for scheduled pipelines

Continuous:  runs forever, processes new data as it arrives
             good for near-real-time requirements

Development: cluster reused between runs, faster iteration,
             errors surface immediately

Production:  fresh cluster each run, automatic retries on failure
```

---

## DDL and DML on Delta Tables

### CREATE TABLE Variants

```sql
-- Fails if table already exists
CREATE TABLE my_table (id INT, name STRING)

-- No-op if table already exists — does not error
CREATE TABLE IF NOT EXISTS my_table (id INT, name STRING)

-- Drops existing table first, then creates — destructive
CREATE OR REPLACE TABLE my_table (id INT, name STRING)

-- Create from query result
CREATE TABLE my_table AS SELECT * FROM source_table

-- Create or replace from query result
CREATE OR REPLACE TABLE my_table AS SELECT * FROM source_table
```

**Exam trap:** `CREATE OR REPLACE TABLE` drops the existing table and its
history before creating new — time travel to pre-replace versions is lost
after this runs. `CREATE TABLE IF NOT EXISTS` preserves the existing table
entirely if it exists.

### ALTER TABLE

```sql
-- Add column
ALTER TABLE my_table ADD COLUMN new_col STRING

-- Rename column (requires column mapping enabled)
ALTER TABLE my_table RENAME COLUMN old_name TO new_name

-- Drop column (requires column mapping enabled)
ALTER TABLE my_table DROP COLUMN unused_col

-- Change column comment
ALTER TABLE my_table ALTER COLUMN id COMMENT 'Primary key'

-- Set table properties
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
```

### INSERT

```sql
-- Insert literal values
INSERT INTO my_table VALUES (1, 'Alice'), (2, 'Bob')

-- Insert from query
INSERT INTO my_table SELECT * FROM source_table

-- Insert overwrite — replaces ALL data in table
INSERT OVERWRITE my_table SELECT * FROM source_table
```

**Exam trap:** `INSERT OVERWRITE` replaces the entire table's data, not just
matching partitions, unless dynamic partition overwrite mode is enabled.

### UPDATE and DELETE

```sql
-- Update matching rows
UPDATE my_table SET status = 'inactive' WHERE last_login < '2024-01-01'

-- Delete matching rows
DELETE FROM my_table WHERE status = 'deleted'
```

These work on Delta tables because Delta supports row-level mutations —
unlike plain Parquet which is immutable per file.

### MERGE INTO (Upsert)

The most exam-relevant DML statement. Combines insert, update, and delete
based on a join condition.

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED AND source.is_deleted = true THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET target.amount = source.amount,
             target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (id, amount, updated_at)
  VALUES (source.id, source.amount, source.updated_at)
```

```
Matching logic:
  Row exists in both target and source, source marked deleted ──► DELETE
  Row exists in both target and source                        ──► UPDATE
  Row exists in source only                                    ──► INSERT
  Row exists in target only                                     ──► unchanged
```

**Common use case:** applying CDC (Change Data Capture) records to a
Silver table — incoming records may be inserts, updates, or deletes.

### PySpark MERGE

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "target_table")

(target.alias("target")
    .merge(
        source_df.alias("source"),
        "target.id = source.id"
    )
    .whenMatchedUpdate(set={
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    })
    .whenNotMatchedInsert(values={
        "id": "source.id",
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    })
    .execute()
)
```

---

## Cluster Configuration for Processing Workloads

### Choosing Node Types

```
Memory-optimized:   large shuffles, caching, wide aggregations
                    use when: groupBy/join on large datasets

Compute-optimized:  CPU-heavy transformations, complex UDFs
                    use when: regex-heavy parsing, custom Python logic

Storage-optimized:  shuffle-heavy workloads with large spill to disk
                    use when: data significantly exceeds cluster memory

GPU-accelerated:    deep learning, ML training
                    use when: TensorFlow/PyTorch model training
```

### Autoscaling

```
Min workers:  baseline capacity, always running
Max workers:  ceiling for scale-up during heavy load

Autoscaling adds workers when:
  Task queue backs up
  Executors are fully utilized

Autoscaling removes workers when:
  Executors idle for a period (default 150 seconds)
```

### Cluster Policies

Cluster policies in Unity Catalog restrict what configurations users
can choose — enforcing cost controls and compliance:

```
Example policy restrictions:
  Max workers: 10
  Allowed node types: limited list
  Auto-termination: required, max 120 minutes
  Spot instances: required for non-production
```

---

## PySpark Transformations and Aggregations (Recap)

This overlaps heavily with the Spark Associate exam. Quick recap of
exam-relevant patterns:

```python
from pyspark.sql.functions import col, count, sum, avg, max, min, \
    countDistinct, approx_count_distinct, when, coalesce

# Conditional column — when/otherwise
df.withColumn("category",
    when(col("amount") > 1000, "high")
    .when(col("amount") > 100, "medium")
    .otherwise("low")
)

# Coalesce — first non-null value across columns
df.withColumn("contact", coalesce(col("email"), col("phone"), col("address")))

# Aggregations
df.groupBy("category").agg(
    count("*").alias("total"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
)

# Window functions — running totals, rankings
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, sum as _sum

window_spec = Window.partitionBy("category").orderBy("date")

df.withColumn("running_total",
    _sum("amount").over(window_spec)
)

df.withColumn("rank", rank().over(
    Window.partitionBy("category").orderBy(col("amount").desc())
))
```

---

## Exam Traps

- `CREATE OR REPLACE TABLE` drops history — time travel to pre-replace
  versions is lost
- `CREATE TABLE IF NOT EXISTS` is a no-op if table exists — preserves
  existing data and history
- `INSERT OVERWRITE` replaces the entire table unless dynamic partition
  overwrite is enabled
- `MERGE INTO` is the standard pattern for CDC/upsert — know the
  WHEN MATCHED / WHEN NOT MATCHED syntax
- DLP expectation conditions describe GOOD data — violations are
  records that DON'T match the condition
- `dlt.read()` = batch, `dlt.read_stream()` = streaming — using the
  wrong one on a streaming source can cause errors or unexpected behavior
- Bronze is append-only and never transformed — faithful copy of source
- Gold tables are denormalized and consumption-pattern specific —
  not a single canonical "gold" schema