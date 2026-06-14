# Walkthrough — Auto Loader Bronze Ingestion

Hands-on exercise building a Bronze ingestion pipeline with Auto Loader
on Databricks Community Edition. Dataset: `/databricks-datasets/wine-quality/`
(red wine quality CSV, 6513 rows, 12 numeric columns, `;` delimiter,
column names contain spaces — e.g. `fixed acidity`).

---

## Step 1 — Explore the source data first

Before writing any pipeline code, understand what you're ingesting:

```python
display(dbutils.fs.ls("dbfs:/databricks-datasets/wine-quality/"))

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load("dbfs:/databricks-datasets/wine-quality/winequality-red.csv")

df.printSchema()
display(df.limit(10))
print(f"Row count: {df.count()}")
```

**Findings:**
- Delimiter is `;` not `,` — easy to miss, breaks everything downstream if wrong
- Column names contain spaces (`fixed acidity`, `volatile acidity`, etc.)
- 12 columns, all numeric, `quality` is an integer 3-9

---

## Step 2 — Hit the DBFS root permission error

First attempt used `dbfs:/tmp/...` for checkpoint and schema storage:

```python
checkpoint_path = "dbfs:/tmp/checkpoints/bronze_wine/"
schema_path     = "dbfs:/tmp/schemas/bronze_wine/"
```

**Error:**
```
[DBFS_DISABLED] Public DBFS root is disabled. Access is denied on path:
/tmp/schemas/wine/bronze/_schemas SQLSTATE: 56038
```

**Cause:** newer Databricks workspaces disable public DBFS root writes,
pushing toward Unity Catalog Volumes for storage.

**Fix — create a UC Volume:**

```python
# Find your actual catalog first — "main" doesn't exist on Community Edition
display(spark.sql("SHOW CATALOGS"))
# Returned: dbacademy, samples, system, workspace

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.wine_pipeline")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.wine_pipeline.checkpoints")
```

Volumes use a different path syntax than DBFS:

```python
# DBFS path:    dbfs:/tmp/...
# Volumes path: /Volumes/catalog/schema/volume/...

source_path     = "dbfs:/databricks-datasets/wine-quality/"
checkpoint_path = "/Volumes/workspace/wine_pipeline/checkpoints/bronze_wine/"
schema_path     = "/Volumes/workspace/wine_pipeline/checkpoints/bronze_wine_schema/"
bronze_table    = "workspace.wine_pipeline.bronze_wine_quality"
```

---

## Step 3 — Hit the invalid column names error

```
[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES] Found invalid character(s)
among ' ,;{}()\n\t=' in the column names of your schema. Invalid column
names: Wine Quality Data Set, fixed acidity, volatile acidity, ...
SQLSTATE: 42K05
```

**Cause:** Delta rejects column names with spaces. `cloudFiles` schema
inference reads the raw CSV header (with spaces) and tries to persist
that schema to `schemaLocation` before any transformation runs.

### First fix attempted — rename after load via `.transform()`

```python
def clean_column_names(df):
    columns = df.columns  # compute ONCE — see note below
    for c in columns:
        clean = c.replace(" ", "_").replace(",", "").replace(";", "")
        if clean != c:
            df = df.withColumnRenamed(c, clean)
    return df

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .option("delimiter", ";")
    .load(source_path)
    .transform(clean_column_names)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)
```

**This still failed with the same error.** `cloudFiles.schemaLocation`
infers and writes the schema (with spaces) to disk BEFORE `.transform()`
ever runs — the rename happens too late in the pipeline.

> **Linter note:** the original loop wrote `for c in df.columns:` and
> reassigned `df` inside the loop. Accessing `.columns` on a new DataFrame
> object every iteration triggers a separate Analyze RPC each time.
> Computing `columns = df.columns` once before the loop avoids this —
> good practice for any streaming DataFrame schema inspection.

### Working fix — explicit schema, bypass inference entirely

```python
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

wine_schema = StructType([
    StructField("fixed_acidity",        DoubleType(),  True),
    StructField("volatile_acidity",     DoubleType(),  True),
    StructField("citric_acid",          DoubleType(),  True),
    StructField("residual_sugar",       DoubleType(),  True),
    StructField("chlorides",            DoubleType(),  True),
    StructField("free_sulfur_dioxide",  DoubleType(),  True),
    StructField("total_sulfur_dioxide", DoubleType(),  True),
    StructField("density",              DoubleType(),  True),
    StructField("pH",                   DoubleType(),  True),
    StructField("sulphates",            DoubleType(),  True),
    StructField("alcohol",              DoubleType(),  True),
    StructField("quality",              IntegerType(), True)
])

# Clear previous failed attempts before retrying
dbutils.fs.rm(checkpoint_path, recurse=True)
dbutils.fs.rm(schema_path, recurse=True)

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("delimiter", ";")
    .schema(wine_schema)              # explicit schema — no inference, no spaces
    .load(source_path)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)
```

Note `cloudFiles.schemaLocation` was removed entirely — it's only
needed when relying on schema inference.

**Result:** success. Bronze table created, 6513 rows.

---

## Step 4 — Verify Bronze

```python
bronze_df = spark.table(bronze_table)
print(f"Row count: {bronze_df.count()}")   # 6513
bronze_df.printSchema()
display(spark.sql(f"DESCRIBE HISTORY {bronze_table}"))
```

**Delta history showed two versions:**
```
Version 0 ──► CREATE TABLE (schema registered)
Version 1 ──► STREAMING UPDATE, epochId=0 (6513 rows written)
```

Databricks auto-enabled on the new table:
```
delta.enableDeletionVectors = true
delta.enableRowTracking = true
delta.parquet.compression.codec = zstd
```

---

## Step 5 — Prove idempotency (exactly-once)

Ran the exact same pipeline a second time, unchanged:

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("delimiter", ";")
    .schema(wine_schema)
    .load(source_path)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

print(f"Row count: {spark.table(bronze_table).count()}")  # still 6513
display(spark.sql(f"DESCRIBE HISTORY {bronze_table}"))     # no Version 2
```

**Result:** row count unchanged, no new Delta version appeared. The
checkpoint had already recorded every file path as processed — Auto
Loader compared the source listing against checkpoint state, found
nothing new, and exited cleanly without touching the Delta table.

This is exactly-once semantics demonstrated directly, not just described.

---

## Key Takeaways

1. **Always check the delimiter and column names of source files before
   writing any pipeline code** — five minutes of `df.printSchema()` saves
   multiple failed pipeline runs.

2. **Explicit schema beats inference when source has invalid characters
   in column names.** Inference persists the bad schema to
   `schemaLocation` before any `.transform()` can fix it. Provide
   `.schema(...)` directly and drop `cloudFiles.schemaLocation` entirely.

3. **Community Edition disables public DBFS root writes.** Use Unity
   Catalog Volumes (`/Volumes/catalog/schema/volume/...`) for checkpoint
   and schema storage instead of `dbfs:/tmp/...`.

4. **Avoid repeated `.columns` calls on streaming DataFrames inside
   loops** — compute once, store in a variable, iterate over the stored
   list.

5. **Idempotency is testable, not just theoretical.** Rerun the identical
   pipeline against the same checkpoint and confirm: same row count, no
   new Delta version.

6. **Use parentheses for chained method calls, not backslash
   continuation** — the Databricks notebook linter flags `\` in chains
   and parentheses-wrapped expressions are the standard style in
   Databricks documentation.