# Section 1: Databricks Intelligence Platform — Practice Exam Questions

---

**Q1.** A data engineer needs to run an automated nightly pipeline that processes
500GB of raw data. The pipeline should be cost-efficient and the compute should
not persist after the job completes. Which compute type is most appropriate?

A. All-Purpose Cluster — always available for immediate execution
B. SQL Warehouse — optimized for large data processing
C. Job Cluster — spins up for the job and terminates on completion
D. Single Node Cluster — simplifies configuration for large workloads

---

**Q2.** A data analyst runs frequent ad-hoc SQL queries against Delta tables
for BI reporting. Query performance needs to be maximized. Which compute
and runtime combination is most appropriate?

A. All-Purpose Cluster with Standard Runtime
B. Job Cluster with ML Runtime
C. SQL Warehouse with Photon enabled
D. Single Node Cluster with Standard Runtime

---

**Q3.** What does the Photon runtime provide that the Standard runtime does not?

A. Support for Python UDFs with faster row-by-row processing
B. A C++ vectorized query engine that accelerates SQL and Delta Lake reads
C. Automatic ML model deployment and serving capabilities
D. Built-in support for streaming with lower latency than Standard runtime

---

**Q4.** A Delta table has accumulated thousands of small files from a high-frequency
streaming pipeline. Query performance has degraded significantly. Which command
addresses this?

A. `VACUUM my_table`
B. `OPTIMIZE my_table`
C. `REFRESH TABLE my_table`
D. `ANALYZE TABLE my_table`

---

**Q5.** A data engineer runs `VACUUM my_table RETAIN 0 HOURS`. What happens?

A. All unreferenced files are deleted regardless of age
B. The command fails — minimum retention is 168 hours (7 days) unless
   the safety check is explicitly disabled
C. All data files including current version are deleted
D. The Delta log is cleared and the table schema is reset

---

**Q6.** A data engineer drops a managed Delta table with `DROP TABLE my_table`.
What happens to the underlying data files?

A. Data files are moved to a recycle bin location for 30 days
B. Data files are unaffected — only metadata is removed
C. Data files are deleted from cloud storage
D. Data files are archived to cold storage automatically

---

**Q7.** A data engineer drops an external Delta table with `DROP TABLE my_table`.
What happens to the underlying data files?

A. Data files are deleted from cloud storage
B. Data files are unaffected — only the metadata entry is removed from Unity Catalog
C. Data files are moved to the managed storage location
D. Data files are locked and cannot be accessed until the table is recreated

---

**Q8.** Which SQL command queries a Delta table as it existed at version 3?

A. `SELECT * FROM my_table AT VERSION 3`
B. `SELECT * FROM my_table HISTORY AS OF 3`
C. `SELECT * FROM my_table VERSION AS OF 3`
D. `SELECT * FROM my_table SNAPSHOT VERSION = 3`

---

**Q9.** A data engineer wants queries filtering on `customer_id` to skip
irrelevant files automatically. The table has no static partitioning.
Which feature should they use?

A. VACUUM with customer_id as the target column
B. OPTIMIZE with ZORDER BY (customer_id)
C. REFRESH TABLE with predicate pushdown enabled
D. ANALYZE TABLE to collect column statistics

---

**Q10.** What is the key difference between Liquid Clustering and ZORDER?

A. ZORDER uses machine learning to optimize layout; Liquid Clustering uses
   static rules
B. Liquid Clustering automatically rebalances clustering as data changes
   and replaces both static partitioning and ZORDER; ZORDER requires
   manual re-running after significant data changes
C. Liquid Clustering only works on streaming tables; ZORDER works on batch tables
D. They are identical — Liquid Clustering is just the new name for ZORDER

---

**Q11.** A data engineer needs hands-off compute that starts instantly with no
cluster configuration, auto-scales, and charges only for actual compute used.
Which option is most appropriate?

A. All-Purpose Cluster with autoscaling enabled
B. High Concurrency Cluster
C. Serverless Compute
D. Job Cluster with spot instances

---

**Q12.** What does the Delta transaction log (`_delta_log`) store?

A. The actual data files in Parquet format
B. JSON files recording every transaction — what files were added/removed,
   schema at each version, and full transaction history
C. Query execution plans for performance optimization
D. Cluster configuration and job scheduling metadata

---

## Answer Key

| Q | Answer | Key Concept |
|---|---|---|
| Q1 | C | Job clusters terminate after job completion — cost efficient for production |
| Q2 | C | SQL Warehouse + Photon for BI/SQL analytics workloads |
| Q3 | B | Photon = C++ vectorized engine, accelerates SQL and Delta reads |
| Q4 | B | OPTIMIZE compacts small files — addresses small file problem |
| Q5 | B | VACUUM minimum retention is 7 days — safety check must be disabled to go lower |
| Q6 | C | DROP TABLE on managed table deletes data files |
| Q7 | B | DROP TABLE on external table removes metadata only — data files untouched |
| Q8 | C | `VERSION AS OF n` is correct Delta time travel syntax |
| Q9 | B | OPTIMIZE ZORDER BY co-locates data for data skipping on filter columns |
| Q10 | B | Liquid Clustering auto-rebalances and replaces both partitioning and ZORDER |
| Q11 | C | Serverless = instant start, no config, auto-scale, pay per use |
| Q12 | B | Delta log = JSON transaction files recording all changes |
