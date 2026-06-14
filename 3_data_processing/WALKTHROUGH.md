# Walkthrough — Medallion Pipeline: Manual then Delta Live Pipeline

Continuation from the Bronze ingestion built in Section 2's walkthrough.
This walkthrough builds Silver and Gold manually, then rebuilds the
entire three-layer pipeline declaratively as a Delta Live Pipeline (DLP),
including a real bug encountered with data quality expectations.

**Starting point:** `workspace.wine_pipeline.bronze_wine_quality`,
6513 rows, explicit schema, 12 numeric columns including `quality` (3-9).

---

## Part 1 — Manual Silver and Gold

### Step 1 — Silver: deduplicate and apply quality filters

```python
from pyspark.sql.functions import col, avg, count, round

bronze_df = spark.table("workspace.wine_pipeline.bronze_wine_quality")

silver_df = (bronze_df
    .dropDuplicates()
    .filter((col("quality") >= 0) & (col("quality") <= 10))
    .filter(col("alcohol").isNotNull())
    .filter(col("pH").isNotNull())
)

silver_table = "workspace.wine_pipeline.silver_wine_quality"

silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"Bronze: {bronze_df.count()}")   # 6513
print(f"Silver: {silver_df.count()}")   # 5318
print(f"Removed: {bronze_df.count() - silver_df.count()}")  # 1195
```

> **Style note:** the original code used backslash line continuation
> (`\`) between chained method calls. The notebook editor's linter
> flagged this. Switched to wrapping the whole expression in parentheses
> — this is the standard pattern in Databricks documentation and sample
> code:
>
> ```python
> # Avoid:
> df.dropDuplicates() \
>   .filter(...) \
>   .filter(...)
>
> # Use:
> (df
>     .dropDuplicates()
>     .filter(...)
>     .filter(...)
> )
> ```

### Step 2 — Break down what Silver removed

```python
deduped = bronze_df.dropDuplicates()
print(f"Removed by dedup:          {bronze_df.count() - deduped.count()}")
print(f"Removed by quality filter: {deduped.count() - silver_df.count()}")
```

This separates "how many rows were exact duplicates" from "how many rows
failed the quality range/null checks" — useful for understanding data
quality issues at the source rather than treating the 1195 removed rows
as one undifferentiated number.

### Step 3 — Gold: aggregate by quality rating

```python
gold_df = (spark.table(silver_table)
    .groupBy("quality")
    .agg(
        count("*").alias("wine_count"),
        round(avg("alcohol"), 2).alias("avg_alcohol"),
        round(avg("fixed_acidity"), 2).alias("avg_fixed_acidity"),
        round(avg("pH"), 2).alias("avg_pH"),
        round(avg("sulphates"), 2).alias("avg_sulphates"),
        round(avg("volatile_acidity"), 2).alias("avg_volatile_acidity")
    )
    .orderBy("quality")
)

display(gold_df)

gold_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.wine_pipeline.gold_wine_by_quality")
```

**Result:** 7 rows, one per quality rating (3 through 9). Quality 6 had
the most wines — the mode of the distribution. Alcohol generally trended
upward with quality, with a notable dip around quality 5 — worth a
follow-up query on `avg_volatile_acidity` since volatile acidity is
generally considered a negative quality indicator and may be a stronger
predictor than alcohol for that bucket.

```python
display(spark.table("workspace.wine_pipeline.gold_wine_by_quality"))
```

### Manual Pipeline Summary

```
Source files (DBFS)
    │
    ▼
Bronze ──► Auto Loader, explicit schema, raw faithful copy, 6513 rows
    │
    ▼
Silver ──► dedup + null/range filters, 5318 rows, 1195 removed
    │
    ▼
Gold   ──► aggregated by quality rating, 7 rows
```

```sql
SHOW TABLES IN workspace.wine_pipeline;
```

---

## Part 2 — Converting to a Delta Live Pipeline (ETL Pipeline)

### Step 1 — Navigate the current UI

```
Old expectation:  Workflows → Delta Live Tables → Create Pipeline →
                  point at an existing notebook

Actual flow:      Jobs & Pipelines → Create → ETL Pipeline
                  ──► drops DIRECTLY into a Python file editor
                  ──► this editor IS the pipeline source
                  ──► no separate notebook is created or referenced
```

If "Workflows" isn't in the sidebar, look for **Jobs & Pipelines**. The
Create dropdown shows **Job**, **ETL Pipeline**, **Ingestion Pipeline**.
ETL Pipeline = Delta Live Tables / Lakeflow Declarative Pipelines.

### Step 2 — Write the full pipeline in the editor

```python
import dlt
from pyspark.sql.functions import col, avg, count, round
from pyspark.sql.types import (
    StructType, StructField, DoubleType, IntegerType
)

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


@dlt.table(
    name="bronze_wine_quality",
    comment="Raw wine quality data ingested from DBFS via Auto Loader"
)
def bronze_wine_quality():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ";")
        .schema(wine_schema)
        .load("/databricks-datasets/wine-quality/")
    )


@dlt.table(
    name="silver_wine_quality",
    comment="Deduplicated and quality-validated wine data"
)
@dlt.expect("valid_quality_range", "quality BETWEEN 0 AND 10")
@dlt.expect_or_drop("non_null_alcohol", "alcohol IS NOT NULL")
@dlt.expect_or_drop("non_null_pH", "pH IS NOT NULL")
def silver_wine_quality():
    return (dlt.read_stream("bronze_wine_quality")
        .dropDuplicates()
    )


@dlt.table(
    name="gold_wine_by_quality",
    comment="Average chemical properties by wine quality rating"
)
def gold_wine_by_quality():
    return (dlt.read("silver_wine_quality")
        .groupBy("quality")
        .agg(
            count("*").alias("wine_count"),
            round(avg("alcohol"), 2).alias("avg_alcohol"),
            round(avg("fixed_acidity"), 2).alias("avg_fixed_acidity"),
            round(avg("pH"), 2).alias("avg_pH"),
            round(avg("sulphates"), 2).alias("avg_sulphates"),
            round(avg("volatile_acidity"), 2).alias("avg_volatile_acidity")
        )
        .orderBy("quality")
    )
```

**Important:** don't try to "Run" this file directly like a regular
notebook cell. `dlt` only exists inside a pipeline execution context —
it runs when the PIPELINE executes, not when the file is run in isolation.

### Step 3 — Configure destination and run

Set destination (via Settings/gear if not prompted automatically):
```
Catalog:        workspace
Target schema:  wine_dlp
```

Hit **Run**/**Start**. A DAG view appears with three connected nodes:
```
bronze_wine_quality ──► silver_wine_quality ──► gold_wine_by_quality
```

This DAG was inferred automatically from the `dlt.read_stream()` and
`dlt.read()` calls — no manual orchestration was defined anywhere.

---

## Part 3 — Debugging: Silver and Gold Showed 0 Rows

### First run result

```
bronze_wine_quality:  6.5K output records   ✓
silver_wine_quality:  0 output records      ✗
gold_wine_by_quality: 0 output records      ✗
```

Bronze matched the manual pipeline exactly. Silver and Gold at zero
pointed to the `@dlt.expect*` decorators.

### Root cause — inverted expectation condition

What was written:

```python
# WRONG
@dlt.expect_or_drop("non_null_alcohol", "alcohol IS NULL")
```

### The mental model for `@dlt.expect*` conditions

**The condition describes what GOOD data looks like.** Records that
DON'T match the condition are violations — and for `expect_or_drop`,
violations get dropped.

```
"alcohol IS NULL"
  ──► says: good data HAS alcohol = null   (backwards intent)
  ──► records WHERE alcohol IS NOT NULL are violations
  ──► expect_or_drop removes them
  ──► alcohol is never null in this dataset
  ──► EVERY row was a "violation" and got dropped
  ──► Silver = 0 rows ──► Gold = 0 rows (no input to aggregate)
```

### The fix

```python
@dlt.expect_or_drop("non_null_alcohol", "alcohol IS NOT NULL")
```

### Re-run after fix

```
bronze_wine_quality:  6.5K records (unchanged — already processed,
                       streaming source with checkpoint)
silver_wine_quality:  5.3K records  ✓
gold_wine_by_quality: 7 records     ✓
```

Matches the manual pipeline exactly: 5318 in Silver, 7 in Gold.

---

## Manual vs DLP — Side by Side

```
Manual pipeline:                    DLP pipeline:
─────────────────────────────────────────────────────────────────
Imperative — controlled each step   Declarative — described desired tables
Checkpoint paths managed manually    Managed automatically
Schema locations managed manually    Managed automatically
No data quality framework            @dlt.expect / expect_or_drop /
                                      expect_or_fail with monitoring UI
                                      showing pass/fail counts per rule
Execution order = code order         Execution order inferred from
                                      dlt.read()/dlt.read_stream() deps
No visual DAG                        Visual DAG with per-table metrics
```

---

## Key Takeaways

1. **`@dlt.expect*` conditions describe GOOD data.** Read every condition
   as "rows that DON'T match this are violations" — never as "this is
   the rule that triggers a drop." This single inversion silently zeroed
   out two downstream tables with no error or failure reported.

2. **A "successful" pipeline run can still be completely wrong.** No
   exception was thrown — the pipeline reported success with 0 rows in
   Silver and Gold. Always check row counts per table after a run, not
   just overall pipeline status.

3. **DLP infers its DAG from `dlt.read()` / `dlt.read_stream()` calls.**
   No manual orchestration is defined — the dependency graph in the UI
   is generated purely from which tables reference which other tables.

4. **`dlt.read()` = batch, `dlt.read_stream()` = streaming.** Silver used
   `read_stream` from Bronze (incremental); Gold used `read` from Silver
   (full current state for aggregation).

5. **Current UI: "ETL Pipeline" creation goes straight to a code editor.**
   There is no intermediate "create a notebook, then attach a pipeline"
   step — the editor IS the pipeline source.

6. **Multiple `@dlt.expect*` decorators stack on one table** — each is
   tracked independently in the monitoring UI with its own pass/fail
   counts, which is exactly where this bug would have been visible
   immediately if checked before assuming success.