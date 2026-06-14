# Walkthrough — Lakeflow Job: Orchestrating the DLP Pipeline

Continuation from the Delta Live Pipeline built in Section 3's walkthrough
(`workspace.wine_dlp.bronze_wine_quality` → `silver_wine_quality` →
`gold_wine_by_quality`, 6513 → 5318 → 7 rows). This walkthrough wraps that
pipeline in a scheduled Lakeflow Job with a dependent validation task,
including a real integration bug between the pipeline output and the
downstream consumer.

---

## Step 1 — Create the Job

```
Jobs & Pipelines → Create → Job
```

**Task 1 ("Main"):**
```
Type:     ETL Pipeline
Pipeline: Wine Quality Pipeline  (the DLP from Section 3)
```

**Schedule:**
```
Daily at 06:00, America/Denver
```

> **Note on Community Edition:** there is no Cluster configuration
> section visible when creating the job. This is not a missing feature —
> Community Edition runs jobs in a serverless-equivalent mode where
> Databricks manages compute automatically. The exam distinguishes Job
> Cluster (4-8 min spin-up, you configure it, terminates after the job)
> from Serverless (seconds to start, no configuration, auto-scales).
> Community Edition is effectively always giving you the serverless
> experience regardless of what it's called in the UI.

---

## Step 2 — Add a dependent validation task

Created a new notebook `wine_validation`:

```python
gold_df = spark.table("workspace.wine_dlp.gold_wine_by_quality")

row_count = gold_df.count()
print(f"Gold table row count: {row_count}")

# Fail the task if Gold table is empty or has unexpected row count
assert row_count > 0, "Gold table is empty — pipeline may have failed"
assert row_count == 7, f"Expected 7 quality ratings, got {row_count}"

print("Validation passed.")
```

Added as Task 2 in the job:

```
Task name:   Validation
Type:        Notebook
Notebook:    wine_validation
Depends on:  Main
```

Resulting task graph:

```
Main (ETL Pipeline) ──► Validation (Notebook)
```

Single arrow, left to right — Databricks will not start Validation
until Main completes successfully.

---

## Step 3 — First run: failed on a naming mismatch

Clicked **Run now**. The graph executed:

```
Main         ──► turned green (succeeded)
Validation   ──► turned blue, then RED (failed)
```

**Error in Validation task logs:** table not found / table reference
mismatch. The table name hardcoded in `wine_validation`
(`workspace.wine_dlp.gold_wine_by_quality`) did not match the actual
output table name produced by the DLP pipeline — a naming
inconsistency between the pipeline definition and the validation
notebook that had crept in across the Section 2/3 iterations.

### The debug cycle

```
1. Job shows red — click into the failed run
2. Click into the failed task (Validation)
3. Read the task's logs/error output
4. Identify the table name mismatch
5. Fix the table name in wine_validation
6. Click "Run now" again — re-executes both tasks
```

### Fix applied

Corrected the table reference in `wine_validation` to match the DLP
pipeline's actual registered output table name.

---

## Step 4 — Re-run after fix

```
Main         ──► green (succeeded)
Validation   ──► green (succeeded, started only after Main finished)

Validation output:
  "Gold table row count: 7"
  "Validation passed."
```

Both tasks completed in sequence, not simultaneously — confirming the
`depends_on` relationship was enforced as expected.

---

## What This Demonstrated

```
Task dependency enforcement:
  Validation did not start until Main reached a successful state.
  If Main had failed, Validation would have been SKIPPED
  (not failed) and the job marked failed overall.

Failure isolation and recovery:
  The job failure pointed directly at the failing task.
  Fixed the code in ONE place (the validation notebook).
  Reran the whole job — Main re-executed (DLP pipeline ran again)
  and Validation then succeeded.
  Did not need to manually rebuild or reconfigure anything.

Integration testing value:
  The validation task caught a real bug — a table name that had
  drifted between the pipeline definition and a downstream consumer.
  This is exactly the class of bug that's invisible if you only check
  "did the pipeline run without error" — the DLP pipeline itself
  succeeded both times. The bug was entirely in the consumer.
```

---

## Final State

```
Job: Wine Quality Pipeline Job
  Schedule: 06:00 daily, America/Denver
  Task 1 — Main:       ETL Pipeline (Bronze → Silver → Gold)
  Task 2 — Validation: Notebook, depends_on Main
                        asserts Gold row count == 7

Tables produced:
  workspace.wine_dlp.bronze_wine_quality   (6513 rows)
  workspace.wine_dlp.silver_wine_quality   (5318 rows)
  workspace.wine_dlp.gold_wine_by_quality  (7 rows)
```

---

## Key Takeaways

1. **Downstream tasks wait for upstream success, not just completion** —
   `depends_on` enforces a true success-gated dependency. A failed Main
   would have produced a SKIPPED Validation, not a parallel/independent
   run.

2. **Job failures are debugged at the task level.** The job-level status
   tells you something failed; clicking into the specific failed task
   gives the actual error. Fix happens at the task's source (notebook,
   pipeline code), then rerun the whole job.

3. **A green pipeline does not guarantee a correct system.** The DLP
   pipeline (Main) succeeded on both runs. The failure was entirely in
   how a downstream consumer referenced the pipeline's output — exactly
   the kind of integration bug that automated validation tasks exist
   to catch.

4. **Community Edition's lack of visible cluster configuration is the
   serverless experience**, not a missing feature. Conceptually map this
   to: Job Cluster = configure + 4-8min startup + terminates after;
   Serverless = no config + seconds + auto-scales. Community Edition
   behaves like the latter.

5. **Rerunning a job after a fix re-executes ALL tasks from the start**
   (Main ran again, not just Validation) — useful to know when reasoning
   about idempotency and cost of "just rerun it" as a fix strategy.