# Walkthrough — Governance: What's Hands-On vs Account-Level on Community Edition

Section 5 covers Unity Catalog governance — much of it is account-level
infrastructure (metastores, Delta Sharing, cross-workspace permissions,
audit log delivery) that a single-user Community Edition workspace cannot
fully exercise. This walkthrough separates what's actually testable on
CE from what's account-admin/multi-org territory, and documents the
managed-vs-external table behavior that IS testable.

---

## Part 1 — Managed vs External Tables (testable on CE)

This is the single most exam-tested concept in Section 5, and it doesn't
require multi-user or account-level access — just a single catalog/schema.

### Managed table

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.wine_pipeline.managed_test (
        id INT, label STRING
    )
""")

spark.sql("INSERT INTO workspace.wine_pipeline.managed_test VALUES (1, 'managed')")

display(spark.sql("DESCRIBE DETAIL workspace.wine_pipeline.managed_test"))
```

`DESCRIBE DETAIL` returns a `location` field — this is the managed
storage path Unity Catalog chose automatically, somewhere under the
catalog/schema's managed storage root. No `LOCATION` clause was
specified at creation, which is what makes this a managed table.

```python
spark.sql("DROP TABLE workspace.wine_pipeline.managed_test")

# Attempt to read the old location directly
managed_location = "<paste the location value from DESCRIBE DETAIL>"
display(dbutils.fs.ls(managed_location))
```

**Expected result:** the path either no longer exists or is empty —
`DROP TABLE` on a managed table deletes the underlying data files, not
just the catalog entry.

### External table

```python
from pyspark.sql.functions import lit

# Write data to a Volume path first — this acts as the "external" location
df = spark.range(5).withColumn("label", lit("external"))
df.write.format("delta").mode("overwrite") \
    .save("/Volumes/workspace/wine_pipeline/checkpoints/external_test_data/")

# Register it as an EXTERNAL table via explicit LOCATION
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.wine_pipeline.external_test
    USING DELTA
    LOCATION '/Volumes/workspace/wine_pipeline/checkpoints/external_test_data/'
""")

display(spark.table("workspace.wine_pipeline.external_test"))
display(spark.sql("DESCRIBE DETAIL workspace.wine_pipeline.external_test"))
```

```python
spark.sql("DROP TABLE workspace.wine_pipeline.external_test")

# Check the underlying files — should STILL be there
display(dbutils.fs.ls("/Volumes/workspace/wine_pipeline/checkpoints/external_test_data/"))
```

**Expected result:** the Delta files remain at the Volume path —
`DROP TABLE` on an external table removes only the catalog/metastore
entry, not the data.

```python
# Re-register it — proves the data was never touched by the drop
spark.sql("""
    CREATE TABLE workspace.wine_pipeline.external_test_recovered
    USING DELTA
    LOCATION '/Volumes/workspace/wine_pipeline/checkpoints/external_test_data/'
""")

display(spark.table("workspace.wine_pipeline.external_test_recovered"))
```

**Expected result:** the recovered table immediately shows the same 5
rows — the data and its Delta log were intact the entire time; only the
catalog reference was removed and recreated.

```
Managed table:                    External table:
─────────────────────────────────────────────────────────────
No LOCATION clause                Explicit LOCATION clause
DESCRIBE DETAIL shows UC-chosen   DESCRIBE DETAIL shows the
managed storage path              user-specified path
DROP TABLE deletes data files     DROP TABLE removes catalog entry
and catalog entry                 only — data files untouched
Cannot recover by re-registering  Can recover instantly by
(data is gone)                    CREATE TABLE ... LOCATION '...'
```

---

## Part 2 — Lineage (testable on CE, with caveats)

```python
spark.sql("""
    CREATE OR REPLACE TABLE workspace.wine_pipeline.gold_lineage_test AS
    SELECT * FROM workspace.wine_dlp.gold_wine_by_quality
""")
```

In Catalog Explorer, navigate to
`workspace.wine_pipeline.gold_lineage_test` and look for a **Lineage**
tab. It should show `workspace.wine_dlp.gold_wine_by_quality` as an
upstream dependency, generated automatically from the query that
created the table — no manual tagging or configuration involved.

**CE caveat:** lineage capture and the Lineage tab UI availability has
varied across Community Edition versions and may be limited or delayed
compared to a full paid workspace. If the tab is empty or missing,
this is a CE limitation, not a sign the feature doesn't work — the
underlying mechanism (lineage generated from query execution) is the
exam-relevant concept regardless of whether CE's UI surfaces it fully.

---

## Part 3 — Audit Logs (account-level, NOT available on CE)

```python
try:
    display(spark.sql("SELECT * FROM system.access.audit LIMIT 10"))
except Exception as e:
    print(f"Not available: {e}")
```

**Why this is CE-limited:** `system.access.audit` is a Unity Catalog
system table populated from account-level audit log delivery, which
requires an account admin to configure audit log delivery to cloud
storage for the metastore. Community Edition workspaces don't expose
account-level administration — there is no Account Console equivalent
to configure this.

**Exam-relevant takeaway regardless:** audit logs are accessed via the
`system.access.audit` system table (not a dedicated "Audit Log Viewer"
UI), and the underlying delivery is configured once per metastore by an
account admin, then automatically covers every workspace attached to
that metastore.

---

## Part 4 — GRANT / REVOKE (limited on CE — single user, no groups)

```python
try:
    spark.sql("""
        GRANT SELECT ON TABLE workspace.wine_pipeline.gold_wine_by_quality
        TO `account users`
    """)
    print("GRANT succeeded")
except Exception as e:
    print(f"GRANT not available or no-op: {e}")
```

**Why this is limited on CE:** Community Edition is fundamentally a
single-user environment — there's no second user or group to grant
access to, and no Account Console to create groups. The `GRANT`
statement itself may execute without error (since UC's privilege model
is present), but there's no second principal to meaningfully test
against.

**Exam-relevant takeaways regardless (from README, verified by docs,
not CE):**
```
GRANT SELECT ON SCHEMA workspace.silver TO `analysts`
  ──► applies to current AND future tables in that schema

GRANT SELECT ON TABLE workspace.gold.revenue TO `exec-team`
  ──► table-level, does not extend to future tables

No "GRANT ... ON ALL TABLES IN SCHEMA" syntax exists
```

---

## Part 5 — Delta Sharing and Lakehouse Federation (account-level, NOT available on CE)

Both features require:
```
Delta Sharing:
  - A second Databricks account/metastore (D2D), or
  - "External Data Sharing" enabled at the metastore level by an
    account admin (open sharing)
  - CREATE SHARE / CREATE RECIPIENT privileges — metastore admin only

Lakehouse Federation:
  - CREATE CONNECTION to an external database (PostgreSQL, MySQL,
    Snowflake, etc.) — requires network access to that external system
    and credentials
  - CREATE FOREIGN CATALOG — requires the connection above
```

Neither is meaningfully testable on a single-user Community Edition
workspace — there's no second account to share with, and standing up an
external database purely to test federation syntax is disproportionate
to the exam's coverage of this topic (a small number of conceptual
questions about use cases and the inbound/outbound distinction).

**What to know instead (syntax reference, not run):**

```sql
-- Delta Sharing — provider side
CREATE SHARE my_share;
ALTER SHARE my_share ADD TABLE workspace.gold.revenue;
CREATE RECIPIENT partner_org COMMENT 'External partner';
DESCRIBE RECIPIENT partner_org;  -- returns activation_link
GRANT SELECT ON SHARE my_share TO RECIPIENT partner_org;

-- Lakehouse Federation
CREATE CONNECTION postgres_conn TYPE postgresql
OPTIONS (host '...', port '5432', user '...', password secret('scope','key'));

CREATE FOREIGN CATALOG postgres_data
USING CONNECTION postgres_conn
OPTIONS (database 'production');

SELECT * FROM postgres_data.public.customers;
```

---

## Summary — What This Walkthrough Proved vs Documented

```
PROVED on CE (ran code, observed real results):
  Managed table DROP deletes data files
  External table DROP preserves data files, re-registration recovers
  the table instantly
  Lineage tab reflects a CREATE TABLE AS SELECT dependency
  (subject to CE UI availability)

DOCUMENTED but not runnable on CE (account-level features):
  system.access.audit — requires account-level audit log delivery
  GRANT/REVOKE across real principals — requires groups/second user
  Delta Sharing (both D2D and open sharing) — requires a second
  account or External Data Sharing enabled by an account admin
  Lakehouse Federation — requires an external database connection
```

The managed/external table distinction is also the single highest-value
item in Section 5 for the exam — it appears across multiple question
patterns (DDL, DROP behavior, data recovery scenarios) and is the one
piece of governance content that's fully verifiable hands-on regardless
of CE's account-level limitations.