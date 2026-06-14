# Databricks Certified Data Engineer Associate
## Exam Prep — Study Guide

**Exam:** Databricks Certified Data Engineer Associate
**Format:** 45 scored multiple-choice questions, 90 minutes
**Fee:** $200 USD
**No test aides** — nothing allowed
**Validity:** 2 years
**Passing score:** ~70%
**Exam guide version:** July 25, 2025

---

## Prerequisite Knowledge

This certification builds on Apache Spark fundamentals. Before starting,
you should be comfortable with:

- Spark architecture — driver, executors, DAG, stages, tasks
- DataFrame API — transformations, actions, joins, aggregations
- Structured Streaming — output modes, checkpoints, watermarks
- Delta Lake basics — ACID transactions, time travel
- Unity Catalog — three-level namespace, GRANT/REVOKE

If any of these are gaps, complete the Spark Associate cert prep first.

---

## Exam Sections and Priority

| Section | Topic | Priority | Your Gap |
|---|---|---|---|
| 1 | Databricks Intelligence Platform | 🟡 Medium | Low — architecture knowledge transfers |
| 2 | Development and Ingestion | 🔴 High | High — Auto Loader is net new |
| 3 | Data Processing and Transformations | 🔴 High | High — Delta Live Pipelines is net new |
| 4 | Productionizing Data Pipelines | 🔴 High | High — Asset Bundles, Lakeflow Jobs net new |
| 5 | Data Governance and Quality | 🟡 Medium | Medium — Delta Sharing and Lakehouse Federation net new |

---

## Two Week Study Plan

```
Days 1-2:   Section 1 — Databricks Intelligence Platform
Days 3-4:   Section 2 — Development and Ingestion
Days 5-7:   Section 3 — Data Processing and Transformations
Days 8-9:   Section 4 — Productionizing Data Pipelines
Days 10-11: Section 5 — Data Governance and Quality
Days 12-13: Full mock exam + targeted drilling
Day 14:     Exam
```

---

## Key Differences From Spark Associate Exam

The Spark Associate exam tested pure PySpark API knowledge.
This exam tests Databricks platform knowledge — how to use
Databricks-specific tools to build production data pipelines.

```
Spark Associate:    How does Spark work?
Data Engineer:      How do you build production pipelines on Databricks?
```

New concepts not covered in Spark Associate:
- Auto Loader — incremental file ingestion
- Delta Live Pipelines (LDP) — declarative pipeline framework
- Databricks Asset Bundles (DAB) — CI/CD for Databricks
- Lakeflow Jobs — workflow orchestration
- Serverless compute — auto-scaling managed compute
- Delta Sharing — open protocol for sharing data externally
- Lakehouse Federation — querying external databases from Databricks
- Unity Catalog — audit logs, lineage, roles in depth

---

## Directory Structure

```
DBDataEngineerAssoc/
├── README.md                           ← this file
├── Section1_DatabricksPlatform/
│   ├── README.md
│   └── EXAM.md
├── Section2_DevAndIngestion/
│   ├── README.md
│   ├── EXAM.md
│   └── WALKTHROUGH.md                  ← Auto Loader Bronze ingestion,
│                                          real errors and fixes
├── Section3_DataProcessing/
│   ├── README.md
│   ├── EXAM.md
│   └── WALKTHROUGH.md                  ← Medallion build (manual) +
│                                          conversion to Delta Live Pipeline,
│                                          expectations bug and fix
├── Section4_Productionizing/
│   ├── README.md
│   ├── EXAM.md
│   └── WALKTHROUGH.md                  ← Lakeflow Job wrapping the DLP
│                                          pipeline, task dependency debug
├── Section5_Governance/
│   ├── README.md
│   ├── EXAM.md
│   └── WALKTHROUGH.md                  ← managed vs external table proof,
│                                          CE limitations for account-level
│                                          governance features
├── Insights/                           ← deep dives on complex topics
└── MockExams/                          ← full 45-question mock exams
    └── Mock1.md
```

---

## Hands-On Walkthroughs

Sections 2, 3, 4, and 5 include `WALKTHROUGH.md` files documenting an
actual end-to-end build on Databricks Community Edition — a wine quality
medallion pipeline (Bronze → Silver → Gold), first built manually, then
converted to a Delta Live Pipeline, then wrapped in a scheduled Lakeflow
Job, plus a governance walkthrough covering what is and isn't testable
on a single-user CE workspace.

Each walkthrough includes the real errors encountered (DBFS permission
denial, invalid Delta column names, an inverted data quality expectation
that silently zeroed two tables, a task naming mismatch) and how each was
diagnosed and fixed. These are read alongside the section README — the
README gives the concept, the walkthrough shows it breaking and getting
fixed in practice.

```
Section 2 WALKTHROUGH:  Bronze ingestion via Auto Loader
                         DBFS_DISABLED → UC Volumes
                         DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES →
                           explicit schema
                         Idempotency proof — rerun produces zero new rows

Section 3 WALKTHROUGH:  Silver/Gold manual build → DLP conversion
                         Inverted @dlt.expect_or_drop condition →
                           silent 0-row Silver/Gold
                         Manual vs DLP side-by-side comparison

Section 4 WALKTHROUGH:  Lakeflow Job — Main (ETL Pipeline) → Validation
                         (Notebook), depends_on enforcement
                         Table naming mismatch between pipeline and
                           consumer — debug and fix cycle

Section 5 WALKTHROUGH:  Managed vs external table DROP behavior —
                           proved hands-on with DESCRIBE DETAIL and
                           file system checks
                         Account-level features (Delta Sharing,
                           Lakehouse Federation, audit logs, multi-
                           principal GRANT) documented as CE-limited
                           with syntax reference
```

---

## Sample Question Analysis

From the official exam guide — these reveal the question style:

**Q1 (DataFrame aggregations):** Tests `count_distinct` vs `sum` vs `count`
on groupBy — same territory as Spark Associate Section 3.

**Q2 (Unity Catalog GRANT):** Tests exact SQL syntax for granting schema
permissions — `GRANT SELECT ON SCHEMA` not `ON ALL TABLES IN SCHEMA`.

**Q3 (Delta Sharing):** Tests understanding of internal vs external sharing
permissions — READ only for external, READ/WRITE for internal via UC.

**Q4 (DDL):** Tests `CREATE OR REPLACE TABLE` vs `CREATE TABLE IF NOT EXISTS`
— knows the difference and when to use each.

**Q5 (DML):** Tests `INSERT INTO table VALUES` syntax — exact SQL.

**Pattern:** Questions are scenario-based. They give you a real engineering
situation and ask which code or command solves it correctly.