# Section 5: Data Governance and Quality — Practice Exam Questions

---

**Q1.** A data engineer creates a table with:
```sql
CREATE TABLE workspace.silver.orders (id INT, amount DOUBLE)
```
No LOCATION clause is specified. What type of table is created, and what
happens to the data if `DROP TABLE workspace.silver.orders` is run later?

A. External table — only metadata is removed, data files remain
B. Managed table — data files are deleted from cloud storage
C. Managed table — data is moved to a recycle bin for 30 days
D. External table — data files are deleted from cloud storage

---

**Q2.** A data engineer wants analysts to be able to SELECT from all
tables currently in the `workspace.gold` schema, including any tables
added to that schema in the future. Which statement achieves this with
a single grant?

A. `GRANT SELECT ON ALL TABLES IN SCHEMA workspace.gold TO analysts`
B. `GRANT SELECT ON SCHEMA workspace.gold TO analysts`
C. `GRANT SELECT ON CATALOG workspace TO analysts`
D. Analysts must be granted SELECT individually on each table

---

**Q3.** Which Unity Catalog role can grant or revoke privileges on any
object within a specific catalog, but not across the entire metastore?

A. Account Admin
B. Metastore Admin
C. Catalog Owner
D. Workspace Admin

---

**Q4.** How are Unity Catalog audit logs primarily accessed for analysis?

A. A dedicated "Audit Log Viewer" UI in the workspace sidebar
B. The `system.access.audit` system table, queryable via SQL
C. Email reports sent daily to the account admin
D. Audit logs are not available in Unity Catalog — only in legacy Hive metastore

---

**Q5.** A data engineer changes the data type of a column in a Silver
table. Which Unity Catalog feature helps identify which downstream
Gold tables and dashboards might be affected?

A. Audit logs
B. Lineage
C. Delta Sharing
D. Cluster policies

---

**Q6.** A company wants to share a Delta table with a partner organization
that does NOT use Databricks. Which Delta Sharing approach is appropriate,
and what is the key limitation?

A. Databricks-to-Databricks sharing — full read/write access
B. Open sharing — read-only access via open protocol/connector,
   recipient does not need Databricks (requires External Data
   Sharing enabled on the provider's metastore)
C. Lakehouse Federation — partner queries your catalog directly
D. Export to Parquet and upload to the partner's cloud storage manually

---

**Q7.** A data engineer at a company using AWS wants to query a Snowflake
database directly from Databricks without copying the data into Delta.
Which feature is appropriate?

A. Delta Sharing — create a share for the Snowflake database
B. Auto Loader — ingest Snowflake tables incrementally
C. Lakehouse Federation — create a foreign catalog connected to Snowflake
D. Databricks Connect — query Snowflake via local IDE

---

**Q8.** What is the key directional difference between Lakehouse
Federation and Delta Sharing?

A. They are functionally identical — different names for the same feature
B. Lakehouse Federation is for querying external sources FROM Databricks
   (inbound); Delta Sharing is for sharing your Databricks data TO others
   (outbound)
C. Lakehouse Federation only works within Unity Catalog; Delta Sharing
   works with Hive metastore only
D. Lakehouse Federation requires Databricks-to-Databricks; Delta Sharing
   works with any external source

---

**Q9.** A company shares Delta tables with a partner in a different
cloud provider (cross-cloud sharing). What is the primary cost
consideration?

A. Storage costs double because data is duplicated across clouds
B. The recipient's compute costs are billed to the provider
C. The provider incurs cloud egress charges when the recipient
   queries data across the cloud boundary
D. Cross-cloud sharing is not supported — data must be copied first

---

**Q10.** A data engineer registers an existing dataset in cloud storage
as a Unity Catalog table using:
```sql
CREATE TABLE workspace.bronze.legacy_data (id INT, value STRING)
LOCATION 's3://existing-bucket/legacy-data/'
```
What type of table is this, and what happens to the data files if the
table is dropped?

A. Managed table — data files are deleted
B. External table — data files remain in S3, only the metadata
   entry is removed
C. Managed table — data files are moved to the Unity Catalog
   managed storage location
D. External table — data files are deleted and metadata remains

---

**Q11.** Which statement about Unity Catalog lineage is correct?

A. Lineage must be manually configured for each table by adding tags
B. Lineage is automatically captured from query execution —
   no configuration required
C. Lineage only tracks table-to-table relationships, not columns
D. Lineage is only available for tables created via Delta Live Pipelines

---

**Q12.** A metastore admin wants to delegate management of a specific
catalog to a team lead, allowing that team lead to grant and revoke
privileges within that catalog without involving the metastore admin
for every change. What should the metastore admin do?

A. Make the team lead an Account Admin
B. Make the team lead the Catalog Owner for that catalog
C. Grant the team lead USE CATALOG and SELECT on all tables individually
D. This is not possible — only metastore admins can grant privileges

---

## Answer Key

| Q | Answer | Key Concept |
|---|---|---|
| Q1 | B | No LOCATION = managed table; DROP deletes data files |
| Q2 | B | GRANT SELECT ON SCHEMA covers current and future tables |
| Q3 | C | Catalog Owner manages privileges within their catalog only |
| Q4 | B | system.access.audit system table is the primary access pattern |
| Q5 | B | Lineage tracks table/column dependencies for impact analysis |
| Q6 | B | Open sharing = read-only, no Databricks needed, requires External Data Sharing enabled |
| Q7 | C | Lakehouse Federation = foreign catalog for querying external sources |
| Q8 | B | Federation = inbound (query external); Delta Sharing = outbound (share yours) |
| Q9 | C | Cross-cloud sharing incurs provider-side egress charges |
| Q10 | B | LOCATION clause = external table; DROP removes metadata only |
| Q11 | B | Lineage is automatic, generated from query execution |
| Q12 | B | Catalog Owner role enables delegated self-service governance |