# Section 1: Databricks Intelligence Platform

## What Is the Databricks Data Intelligence Platform

Databricks is a unified analytics platform built on top of Apache Spark,
combining a data lakehouse architecture with collaborative notebooks,
managed compute, and AI/ML capabilities.

The platform sits on top of cloud object storage (S3, ADLS, GCS) and
provides a single place for data engineering, data science, and analytics
workloads — eliminating the need for separate data warehouse, data lake,
and ML platform tools.

```
┌─────────────────────────────────────────────────────────┐
│              Databricks Platform                        │
│                                                         │
│  Notebooks  │  Jobs  │  SQL  │  ML  │  Delta Live       │
│─────────────────────────────────────────────────────────│
│              Unity Catalog (Governance)                 │
│─────────────────────────────────────────────────────────│
│              Delta Lake (Storage Layer)                 │
│─────────────────────────────────────────────────────────│
│         Cloud Object Storage (S3 / ADLS / GCS)         │
└─────────────────────────────────────────────────────────┘
```

---

## The Lakehouse Architecture

The Lakehouse combines the best of data lakes and data warehouses:

```
Data Lake:        cheap object storage, any format, no ACID
Data Warehouse:   ACID transactions, schema enforcement, fast queries
                  but expensive and siloed

Lakehouse:        cheap object storage (lake cost)
                  ACID transactions via Delta Lake (warehouse reliability)
                  open formats — no vendor lock-in
                  supports BI, ML, and streaming on same data
```

**Delta Lake** is the storage layer that makes the lakehouse work:
- ACID transactions on object storage
- Schema enforcement and evolution
- Time travel — query data as of any point in time
- Unified batch and streaming

---

## Databricks Workspace

The workspace is the collaborative environment where all work happens.

Key components:
- **Notebooks** — interactive Python/SQL/Scala/R development
- **Clusters** — compute attached to notebooks or jobs
- **Jobs/Workflows** — scheduled and orchestrated pipelines
- **SQL Warehouses** — compute optimized for SQL analytics
- **Unity Catalog** — unified governance layer
- **DBFS** — Databricks File System (abstraction over cloud storage)
- **Repos** — Git integration for version control

---

## Compute Types

The exam tests which compute to use for which use case.

### All-Purpose Clusters
```
Use for:    interactive development, notebooks, ad-hoc analysis
Billed:     per DBU while running — expensive if left on
Key trait:  manually started/stopped, collaborative (multiple users)
Config:     Standard, Single Node, or High Concurrency mode
```

### Job Clusters
```
Use for:    automated production pipelines, scheduled jobs
Billed:     only while job runs — cost efficient
Key trait:  spun up for job, terminated when done
            cannot attach a notebook interactively
```

### SQL Warehouses (formerly SQL Endpoints)
```
Use for:    Databricks SQL, BI tools, dashboards, ad-hoc SQL queries
Billed:     per DBU while running
Key trait:  optimized for SQL workloads
            supports Photon engine for faster query execution
            serverless option available
```

### Serverless Compute
```
Use for:    notebooks, jobs, SQL — any workload
Key trait:  no cluster configuration needed
            Databricks manages infrastructure entirely
            instant startup — no cluster spin-up wait
            auto-scales automatically
            pay only for compute used
```

**Exam decision tree:**
```
Interactive notebook development    ──► All-Purpose Cluster
Production scheduled pipeline       ──► Job Cluster
SQL analytics / BI dashboard        ──► SQL Warehouse
Hands-off, no config wanted         ──► Serverless
```

---

## Cluster Configuration

### Cluster Modes
```
Standard:          multi-user, shared cluster, full Spark features
Single Node:       driver only, no workers — small data, ML libraries
High Concurrency:  optimized for many concurrent users
                   (legacy — largely replaced by serverless)
```

### Cluster Sizing
```
Driver node:   coordinates job execution, holds SparkSession
               size based on: number of concurrent jobs, collect() calls
               larger if: many notebooks attached, large results collected

Worker nodes:  run executors, process data
               size based on: data volume, transformation complexity
               more workers = more parallelism

Memory optimized:  large datasets, caching, ML model serving
Compute optimized: CPU-intensive transformations, complex aggregations
Storage optimized: shuffle-heavy workloads, large spills to disk
GPU accelerated:   deep learning, ML training
```

### Databricks Runtime
```
Standard Runtime:     Spark + Delta Lake + common libraries
ML Runtime:           Standard + MLflow + ML libraries (sklearn, TF, PyTorch)
Photon Runtime:       C++ vectorized query engine — faster SQL, Delta reads
                      same API as standard, transparent acceleration
```

**Photon** is important for the exam:
- Accelerates Delta Lake reads, SQL queries, aggregations
- Transparent — same PySpark/SQL code, faster execution
- Best for: ETL workloads, SQL analytics, large scans
- Not beneficial for: Python UDFs (still row-by-row), streaming state ops

---

## Query Optimization Features

### Delta Lake Optimizations

**OPTIMIZE**
```sql
OPTIMIZE my_table
```
Compacts small files into larger ones. Addresses the small file problem
that accumulates from streaming writes or frequent small batch appends.

**OPTIMIZE with ZORDER**
```sql
OPTIMIZE my_table ZORDER BY (customer_id, date)
```
Co-locates related data in the same files. Dramatically improves query
performance when filtering on the ZORDERed columns. Similar concept to
an index but implemented as file layout optimization.

```
Without ZORDER:  customer_id values scattered across all files
                 query must scan all files to find customer_id = 'C001'

With ZORDER:     all rows for customer_id = 'C001' co-located in few files
                 query skips most files via data skipping statistics
```

**VACUUM**
```sql
VACUUM my_table RETAIN 168 HOURS  -- 7 days default
```
Removes old files no longer referenced by the Delta log.
Older versions become inaccessible after VACUUM runs.
Default retention: 7 days (168 hours). Cannot set below 7 days
without disabling the safety check.

**Auto Optimize (Auto Compaction + Optimized Writes)**
```python
# Table property — enables automatic file compaction
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```
Databricks automatically runs compaction after writes. Eliminates need
for manual OPTIMIZE calls in most streaming and batch pipelines.

### Liquid Clustering (Replaces ZORDER + Static Partitioning)
```sql
CREATE TABLE my_table (
  customer_id STRING,
  date DATE,
  amount DOUBLE
)
CLUSTER BY (customer_id, date);
```
New in Delta Lake 3.0. Replaces both static partitioning and ZORDER.
Automatically rebalances clustering as data changes. No need to choose
partition columns upfront. Exam may test this as the modern alternative.

### Predictive Optimization
Databricks-managed service that automatically runs OPTIMIZE and VACUUM
on tables. No manual scheduling needed. Enabled at the catalog or
schema level in Unity Catalog.

---

## Delta Lake Core Concepts

### ACID Transactions
```
Atomicity:    write either fully succeeds or fully fails — no partial writes
Consistency:  schema enforcement — bad data rejected at write time
Isolation:    concurrent readers/writers don't interfere
Durability:   committed data survives failures
```

### Delta Log
Every Delta table has a `_delta_log` directory containing JSON transaction
files. Every write appends a new entry. This is the source of truth for:
- What files are part of the table
- Schema at each version
- Transaction history

```
my_table/
├── _delta_log/
│   ├── 00000000000000000000.json  ← version 0 — table creation
│   ├── 00000000000000000001.json  ← version 1 — first write
│   └── 00000000000000000002.json  ← version 2 — second write
└── part-00000.parquet
    part-00001.parquet
```

### Time Travel
```sql
-- Query as of version number
SELECT * FROM my_table VERSION AS OF 5

-- Query as of timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-01'

-- PySpark syntax
df = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("/path/to/table")
```

### Managed vs External Tables
```
Managed table:
  Databricks controls BOTH metadata AND data files
  DROP TABLE deletes the data from cloud storage
  Default location: Unity Catalog managed storage

External table:
  Databricks controls metadata only
  Data files live at a user-specified external location
  DROP TABLE deletes metadata only — data files untouched
  Use when: data shared with other systems, pre-existing data
```

**Exam trap:** DROP TABLE on a managed table = data gone.
DROP TABLE on an external table = metadata gone, data safe.

---

## Exam Traps

- Job clusters are for production pipelines — they terminate after the job
- All-purpose clusters are for interactive development — expensive if left running
- Photon accelerates SQL and Delta reads — does NOT help Python UDFs
- VACUUM with retention below 7 days requires disabling the safety check
- DROP TABLE on a managed table destroys the data
- DROP TABLE on an external table only removes the metadata
- ZORDER improves filter performance via data skipping — not a traditional index
- Liquid Clustering is the modern replacement for both partitioning and ZORDER
