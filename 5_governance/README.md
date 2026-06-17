# Section 5: Data Governance and Quality

## Account, Metastore, and Workspace Hierarchy

Before the catalog.schema.table namespace makes sense, you need the layer
above it — how accounts, metastores, and workspaces relate. This is a
common source of confusion because catalogs live ABOVE the workspace
level conceptually, but each workspace only sees a subset of them.

```
Databricks Account                    ← billing entity, top-level
  │   manages: users, groups, workspaces, metastores
  │   accessed via: Account Console (accounts.cloud.databricks.com)
  │
  └── Metastore                       ← ONE per cloud region (typically)
        │   created automatically with the first workspace in a region,
        │   or created manually by an account admin for older accounts
        │   stores: all catalog/schema/table metadata + security config
        │
        ├── attached to ──► Workspace A
        ├── attached to ──► Workspace B
        └── attached to ──► Workspace C   (all in the same region)

        └── Catalog 1, Catalog 2, Catalog 3, ...
              └── Schema
                    └── Table / View / Volume / Function / Model
```

A single Unity Catalog metastore can serve multiple workspaces in the
same region — this is the recommended pattern for consolidating
governance rather than running separate metastores per workspace.

### Two Separate UI Consoles

```
Account Console (accounts.cloud.databricks.com)
  Audience: account admins only
  Manages:
    - Metastores — create, assign to regions, assign workspaces
    - Workspaces — create new ones, see which metastore each uses
    - Users/groups/service principals — account-wide identity
    - Delta Sharing settings — enabled per METASTORE, applies to
      ALL workspaces attached to that metastore
    - Metastore admin assignment

Workspace UI → Catalog Explorer (left sidebar → Catalog)
  Audience: everyone, scoped by binding + grants
  Shows:
    - Only catalogs BOUND to this workspace
    - Within those, only objects the user has GRANT access to
    - Lineage tab, permissions tab, per-object details
```

**Exam-relevant implication:** Delta Sharing's enable/disable toggle and
the "External Data Sharing" feature are configured at the metastore
level in the Account Console — not per workspace. If multiple workspaces
share one metastore, the setting applies to all of them at once.

### Workspace-Catalog Binding — Visibility vs. Permission

This is the most commonly misunderstood part of the hierarchy. Catalogs
live in the metastore, ABOVE any individual workspace. Each workspace
only "sees" catalogs that are bound to it.

```
Binding  ──► "this catalog EXISTS and is visible in my workspace"
GRANT    ──► "I can actually read/write data inside this catalog"

BOTH are required. A catalog that is bound but not granted appears
in Catalog Explorer, but every query against it returns
PERMISSION_DENIED.
```

```
Metastore (us-east-1)
  ├── Catalog: prod_sales ─── bound to ──► Workspace: prod-analytics  ✅ visible
  │                          NOT bound  ──► Workspace: dev-sandbox    ❌ hidden
  │
  ├── Catalog: dev_scratch ── bound to ──► Workspace: dev-sandbox     ✅ visible
  │
  └── Catalog: workspace (auto-created) ── bound ONLY to its own
                                            workspace by default
```

**The default**: a catalog is shared with ALL workspaces attached to the
metastore. **The exception**: the auto-created "workspace" catalog that
comes with every new workspace is bound only to that workspace unless
explicitly extended.

This is why Community Edition shows a catalog literally named
`workspace` — it's the auto-provisioned, workspace-bound default catalog
that ships with every new workspace.

### The Full Access-Check Flow

```
User → Workspace → Metastore → (binding check) → Catalog → Schema → Table
                                       ↑
                                  (grant check)
```

**Exam trap:** if a user in Workspace A cannot see Catalog X even though
Workspace A is attached to the same metastore that contains Catalog X,
the cause is a missing **workspace-catalog binding**, not a GRANT/
permission problem. These are two different failure modes with two
different fixes — binding controls visibility, GRANT controls access
to data once visible.

---

## Unity Catalog Overview

Unity Catalog is Databricks' unified governance layer for data and AI assets
across all workspaces attached to a metastore.

### The Three-Level Namespace

```
catalog.schema.table

Example:
  workspace.wine_dlp.gold_wine_by_quality
  ─────────  ───────  ──────────────────────
  catalog    schema   table

Hierarchy (within a metastore):
  Catalog (top-level container — e.g. prod, dev, sandbox)
    └── Schema (database — e.g. bronze, silver, gold)
          └── Table / View / Volume / Function / Model
```

---

## Managed vs External Tables

This is one of the most heavily tested concepts across BOTH the Spark
Associate and Data Engineer Associate exams.

```
Managed table:
  Unity Catalog controls BOTH metadata AND data files
  Data stored in the catalog/schema's managed storage location
  DROP TABLE deletes the data files from cloud storage
  Default when CREATE TABLE has no LOCATION clause

External table:
  Unity Catalog controls metadata only
  Data files live at a user-specified external location
  DROP TABLE removes metadata only — data files untouched
  Created with explicit LOCATION clause
```

```sql
-- Managed table — UC controls storage location
CREATE TABLE workspace.silver.customers (
  id INT, name STRING
)

-- External table — data lives at specified path
CREATE TABLE workspace.silver.customers (
  id INT, name STRING
)
LOCATION 's3://my-bucket/customers/'
```

**When to use external tables:**
```
Data needs to be accessed by non-Databricks systems
Data already exists at a location and you're registering it
Compliance requires data to remain in a specific location
  regardless of catalog structure
```

**Exam trap:** DROP TABLE on managed = data gone. DROP TABLE on external
= metadata gone, data safe, can be re-registered with CREATE TABLE ... LOCATION.

---

## Unity Catalog Permissions

### GRANT / REVOKE Syntax

```sql
-- USE CATALOG: pure traversal privilege — lets a principal "see into"
-- the catalog. Grants NO data access by itself. SELECT/MODIFY are
-- separate grants required at the schema or table level.
GRANT USE CATALOG ON CATALOG workspace TO `data-engineers`;

-- The complete chain required for analysts to query workspace.gold.revenue:
-- ALL THREE are required — each is a separate gate, none implies the others
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  workspace.gold TO `analysts`;
GRANT SELECT      ON SCHEMA  workspace.gold TO `analysts`;

-- Grant schema-level access
GRANT USE SCHEMA ON SCHEMA workspace.silver TO `analysts`;
GRANT SELECT ON SCHEMA workspace.silver TO `analysts`;

-- Grant table-level access
GRANT SELECT ON TABLE workspace.gold.revenue TO `exec-team`;

-- Grant write access
GRANT MODIFY ON TABLE workspace.silver.customers TO `etl-service-principal`;

-- Revoke access
REVOKE SELECT ON TABLE workspace.gold.revenue FROM `exec-team`;
```

### Permission Inheritance

Two different mechanisms are easy to conflate — gates vs. inheriting grants.

```
GATES (USE CATALOG, USE SCHEMA):
  Pure traversal/visibility privileges.
  Grant NOTHING about data access on their own.
  Required at EVERY level of the path to the object —
  missing any one gate = PERMISSION_DENIED, regardless of
  what's granted elsewhere.

  GRANT USE CATALOG ON CATALOG workspace TO `team`
    ──► team can see the catalog exists
    ──► team STILL cannot query any table — USE CATALOG
        does not grant SELECT, and does not imply USE SCHEMA

INHERITING GRANTS (SELECT ON SCHEMA, SELECT ON CATALOG):
  Granted at a higher level, automatically apply to objects
  below — including objects created LATER.

  GRANT SELECT ON SCHEMA workspace.silver TO `team`
    ──► team can SELECT from ALL tables currently in
        workspace.silver AND any tables added in the future
    ──► but team STILL needs USE CATALOG + USE SCHEMA gates
        satisfied to actually reach workspace.silver
```

**The practical rule:** gates (`USE CATALOG`, `USE SCHEMA`) must be
granted at every level on the path regardless of where the data-access
grant (`SELECT`/`MODIFY`) sits. The data-access grant is what
"inherits downward" to future objects — the gates do not inherit
anything, they're just prerequisites that happen to exist at multiple
levels.

**Exam trap:** `GRANT SELECT ON SCHEMA` is different from granting on each
table individually — schema-level grants apply to future tables too.
There is no `GRANT SELECT ON ALL TABLES IN SCHEMA` syntax — the correct
form is `GRANT SELECT ON SCHEMA`.

### Privilege Types

```
USE CATALOG     ──► required to reference anything inside the catalog
USE SCHEMA      ──► required to reference anything inside the schema
SELECT          ──► read data
MODIFY          ──► insert/update/delete/merge data
CREATE          ──► create new objects within catalog/schema
ALL PRIVILEGES  ──► all of the above
```

---

## Key Roles in Unity Catalog

```
Account Admin:
  Manages the Databricks account — billing, workspace creation
  Highest level — typically IT/cloud admin team

Metastore Admin:
  Manages the Unity Catalog metastore for a region
  Can grant/revoke any privilege on any object in the metastore
  Typically a small central governance team

Catalog Owner:
  Owns a specific catalog
  Can grant/revoke privileges within that catalog
  Typically a team lead or data domain owner

Schema Owner:
  Owns a specific schema within a catalog
  Can grant/revoke privileges within that schema

Workspace Admin:
  Manages a specific workspace — cluster policies, workspace settings
  Separate from Unity Catalog object ownership
```

```
Ownership hierarchy:
  Account Admin
    └── Metastore Admin (per region)
          └── Catalog Owner (per catalog)
                └── Schema Owner (per schema)
                      └── Table Owner (per table)
```

Object owners can grant privileges on their objects without needing
admin involvement — this is how Unity Catalog enables self-service
governance.

---

## Audit Logs

Unity Catalog automatically logs every access and change to governed objects.

```
What's logged:
  Every SELECT, INSERT, UPDATE, DELETE, MERGE
  Every GRANT/REVOKE
  Every CREATE/DROP/ALTER on catalogs, schemas, tables
  Every table read by every user/service principal

Where audit logs are stored:
  Delivered to a cloud storage location configured by the account admin
  AWS: S3 bucket
  Azure: ADLS Gen2
  GCP: GCS bucket
  Format: JSON files, one per event category, partitioned by date

Accessing audit logs:
  System tables — query directly via SQL
  system.access.audit
```

```sql
-- Query audit logs via system table
SELECT *
FROM system.access.audit
WHERE action_name = 'getTable'
  AND user_identity.email = 'analyst@company.com'
ORDER BY event_time DESC
```

**Exam trap:** Audit logs are NOT viewed through a special UI dashboard
by default — they're delivered as files to cloud storage AND queryable
via system tables. The system table approach (`system.access.audit`) is
the modern, exam-relevant access pattern.

---

## Lineage in Unity Catalog

Unity Catalog automatically tracks lineage — which tables, columns, jobs,
and notebooks produced and consumed which data.

```
Lineage tracks:
  Table-to-table     ──► which tables were read to produce this table
  Column-to-column   ──► which source columns produced this column
  Job/notebook       ──► which job or notebook performed the write
  Dashboard          ──► which tables feed which dashboards

Captured automatically:
  No configuration needed
  Generated from query execution — Spark and SQL queries
  Visible in Catalog Explorer — "Lineage" tab on any table
```

**Use cases:**
```
Impact analysis:    "If I change this column, what breaks downstream?"
Debugging:          "Where did this bad data come from?"
Compliance:         "What is the full provenance of this PII column?"
```

---

## Delta Sharing

Delta Sharing is Databricks' umbrella term for an open protocol for sharing
data across organizations and platforms WITHOUT copying data. Under this
umbrella there are two distinct sharing **models**.

### The Two Sharing Models

```
Databricks-to-Databricks sharing:
  Recipient has a Unity Catalog-enabled Databricks workspace
  Within the SAME account: always enabled by default
  ACROSS DIFFERENT accounts: requires "External Data Sharing"
    feature enabled by an account admin
  Supports notebook sharing, UC volume sharing, AI model sharing,
    governance/auditing on both sides

Open sharing:
  Recipient uses ANY tool — Databricks or not
  Via open-source Delta Sharing connectors (Python, Pandas, Spark)
  Also requires "External Data Sharing" enabled on the provider's
    metastore
  READ-ONLY for the recipient
```

### External Data Sharing — The Feature Toggle

```
"External Data Sharing" is NOT a third sharing type.
It is an account-level feature group that an account admin enables
to unlock sharing OUTSIDE the account boundary:

  OFF (default):
    Databricks-to-Databricks sharing works ONLY between metastores
    in the SAME account
    No open sharing, no cross-account D2D sharing

  ON:
    Databricks-to-Databricks sharing works across DIFFERENT accounts
    Open sharing becomes available (any recipient, any tool)
```

```
Same account, both on Unity Catalog:
  ──► Databricks-to-Databricks sharing, always available

Different accounts, both on Unity Catalog:
  ──► Databricks-to-Databricks sharing, requires External Data
      Sharing enabled

Recipient has no Databricks at all:
  ──► Open sharing, requires External Data Sharing enabled,
      recipient gets READ-ONLY access via open connector
```

### Databricks-to-Databricks Sharing — Mechanics

```
Provider (your org)                    Recipient (other org)
┌─────────────────┐                   ┌─────────────────┐
│ Unity Catalog    │                   │ Unity Catalog    │
│ CREATE SHARE     │ ──── share ────►  │ CREATE CATALOG   │
│ ADD TABLE        │                   │   FROM SHARE     │
│ GRANT TO         │                   │                  │
│   recipient      │                   │ Query as if      │
└─────────────────┘                   │ it were a local  │
                                       │ table — no copy  │
                                       └─────────────────┘
```

```sql
-- Provider side — create a share and add tables
CREATE SHARE sales_share;
ALTER SHARE sales_share ADD TABLE workspace.gold.revenue;

-- Grant access to a recipient (another Databricks account)
GRANT SELECT ON SHARE sales_share TO RECIPIENT other_org_recipient;

-- Recipient side — access the shared data as a catalog
CREATE CATALOG shared_sales USING SHARE provider_org.sales_share;
SELECT * FROM shared_sales.gold.revenue;
```

### Open Sharing (Non-Databricks Recipients)

For recipients who don't use Databricks — open source Delta Sharing
clients (Python, Spark, Pandas connectors) can read shared tables
via REST API, no Databricks account needed. Requires External Data
Sharing enabled on the provider's metastore.

```
Databricks-to-Databricks sharing:
  Recipient has Unity Catalog
  Richer feature set — notebook sharing, UC volumes, AI models,
    governance/lineage/auditing on both sides
  Recipient sees shared data as a catalog — native query experience

Open sharing:
  Recipient uses open source connector — no Databricks needed
  READ-ONLY access
  Recipient gets a Delta Sharing "profile file" — credentials + endpoint
  No Unity Catalog features on recipient side
```

**Exam trap:** Databricks-to-Databricks sharing supports richer features
than open sharing. Open sharing via the open protocol is READ ONLY.
"External Data Sharing" is the account-level toggle that enables both
cross-account D2D sharing AND open sharing — don't confuse the toggle
name with the sharing model name.

### Advantages of Delta Sharing

```
No data copying          ──► recipient queries data in place
Live data                 ──► recipient always sees current data,
                              no stale copies to refresh
Cross-platform            ──► recipient doesn't need Databricks
Cross-cloud                ──► works across AWS/Azure/GCP
Centralized access control ──► provider can revoke access instantly
```

### Limitations of Delta Sharing

```
External sharing is read-only
Recipient needs network access to provider's storage/endpoint
Some advanced features (column masking, row filters) may not
  propagate to external recipients depending on configuration
```

### Cost Considerations for Cross-Cloud Sharing

```
Data transfer costs:
  Provider's cloud charges egress fees when recipient queries
  data that crosses cloud boundaries (e.g. AWS to Azure)

Compute costs:
  Recipient pays for their own compute to query shared data
  Provider does NOT pay for recipient's query compute

Storage costs:
  No duplication — provider stores data once
  Recipient has zero storage cost for shared data

Cross-cloud egress is the primary cost factor to discuss with
customers — same-cloud sharing has minimal additional cost,
cross-cloud sharing incurs provider-side egress charges per query.
```

---

## Lakehouse Federation

Lakehouse Federation lets you query external data sources directly from
Unity Catalog WITHOUT moving or copying the data — using a foreign catalog.

```
External Database (PostgreSQL, MySQL, Snowflake, etc.)
         │
         ▼
Unity Catalog Foreign Catalog (federation connection)
         │
         ▼
Query via standard catalog.schema.table syntax
  — Databricks pushes query execution to the source system
```

```sql
-- Create a connection to an external source
CREATE CONNECTION postgres_conn TYPE postgresql
OPTIONS (
  host 'my-postgres-host.com',
  port '5432',
  user 'readonly_user',
  password secret('scope', 'pg-password')
);

-- Create a foreign catalog using that connection
CREATE FOREIGN CATALOG postgres_data
USING CONNECTION postgres_conn
OPTIONS (database 'production');

-- Query as if it were a native Unity Catalog table
SELECT * FROM postgres_data.public.customers;
```

### Supported External Sources

```
PostgreSQL, MySQL, SQL Server, Snowflake, Redshift,
BigQuery, Oracle, and others (growing list)
```

### Use Cases

```
Querying operational databases without ETL pipeline
  ──► join Delta tables with live PostgreSQL data in one query

Avoiding data duplication
  ──► don't need to ingest entire external DB to Bronze
      just to run occasional reports

Gradual migration
  ──► query legacy Snowflake/Redshift tables alongside new
      Delta tables during a platform migration

Single governance layer
  ──► Unity Catalog permissions apply even to external
      foreign catalogs — one place to manage access
```

### Lakehouse Federation vs Delta Sharing — Key Distinction

```
Lakehouse Federation:
  YOU query THEIR data (external source)
  Read access to external systems FROM Databricks
  Query execution may be pushed down to the source system

Delta Sharing:
  THEY query YOUR data (your Delta tables)
  You share YOUR data OUT to other organizations/platforms
  Direction is outbound — you are the provider
```

**Exam trap:** these are opposite directions of data flow. Federation
is inbound (you reading external systems). Delta Sharing is outbound
(others reading your data).

---

## Exam Decision Tree

```
Need to share Delta tables with another company that uses Databricks?
  ──► Delta Sharing — Databricks-to-Databricks (cross-account
      requires External Data Sharing enabled)

Need to share data with a company that doesn't use Databricks?
  ──► Delta Sharing — open sharing (read-only, requires External
      Data Sharing enabled on your metastore)

Need to query a PostgreSQL database from Databricks without
copying data?
  ──► Lakehouse Federation — foreign catalog

DROP TABLE on managed table — what happens to data?
  ──► Data files deleted from cloud storage

DROP TABLE on external table — what happens to data?
  ──► Metadata removed only, data files untouched

Need analysts to SELECT from all current AND future tables
in a schema?
  ──► GRANT SELECT ON SCHEMA (not per-table grants)

Need to know which tables feed a broken dashboard?
  ──► Unity Catalog lineage — Catalog Explorer lineage tab

Need to audit who queried a sensitive table last week?
  ──► system.access.audit system table

User in Workspace A can't see a catalog that exists in the same
metastore Workspace A is attached to?
  ──► Workspace-catalog binding issue, not a GRANT issue —
      check Account Console catalog binding settings

Multiple workspaces need to share one governance setup
(catalogs, permissions, Delta Sharing config)?
  ──► Attach all workspaces to the same metastore — this is the
      recommended consolidation pattern
```

---

## Exam Traps

- Managed table DROP = data deleted. External table DROP = metadata only
- `GRANT SELECT ON SCHEMA` applies to current AND future tables —
  different from granting on each table individually
- No `GRANT ... ON ALL TABLES IN SCHEMA` syntax exists — use schema-level grant
- Audit logs accessible via `system.access.audit` system table
- Lineage is automatic — no configuration required, generated from
  query execution
- External Delta Sharing (open sharing, non-Databricks recipients) is READ-ONLY
- Databricks-to-Databricks Delta Sharing supports richer features
  than open sharing
- "External Data Sharing" is an account-level feature TOGGLE, not a
  sharing model — it enables cross-account D2D sharing AND open sharing
- Cross-cloud Delta Sharing incurs provider-side egress costs;
  same-cloud sharing has minimal additional cost
- Lakehouse Federation = inbound (you query external systems)
- Delta Sharing = outbound (others query your data)
- Foreign catalogs in Lakehouse Federation are governed by Unity
  Catalog permissions like any other catalog
- A metastore is typically one per region and can serve multiple
  workspaces — catalogs live in the metastore, ABOVE individual workspaces
- Workspace-catalog BINDING controls visibility ("does this catalog
  appear in my workspace"); GRANT controls access ("can I query it") —
  both required, and they fail differently
- The auto-created "workspace" catalog is bound ONLY to its own
  workspace by default — this is the exception to "catalogs are shared
  with all workspaces on the metastore by default"
- Delta Sharing enable/disable and External Data Sharing are configured
  at the METASTORE level via the Account Console — applies to every
  workspace attached to that metastore, not per-workspace