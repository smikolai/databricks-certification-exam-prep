# Section 3: Data Processing and Transformations — Practice Exam Questions

---

**Q1.** A data engineer is designing a medallion architecture pipeline.
Which statement correctly describes the Bronze layer?

A. Bronze contains deduplicated, schema-enforced data ready for analysts
B. Bronze contains a raw, append-only, faithful copy of the source data
   with minimal transformation
C. Bronze contains pre-aggregated metrics for BI dashboards
D. Bronze contains only data that has passed all quality checks

---

**Q2.** What is the primary reason for separating a pipeline into
Bronze, Silver, and Gold layers rather than transforming directly
from source to final table?

A. Each layer can use a different programming language
B. Storage costs are lower when data is split across multiple tables
C. Each layer has a single responsibility, can be reprocessed
   independently, and is independently testable
D. Delta Lake requires at least three tables per pipeline

---

**Q3.** In a Delta Live Pipeline, what is the difference between
`dlt.read("orders")` and `dlt.read_stream("orders")`?

A. `dlt.read` reads from cloud storage; `dlt.read_stream` reads from Delta tables
B. `dlt.read` performs a batch read of the full current table state;
   `dlt.read_stream` performs an incremental streaming read of new records
C. `dlt.read` is for Python pipelines; `dlt.read_stream` is for SQL pipelines
D. They are identical — `dlt.read_stream` is deprecated

---

**Q4.** A data engineer writes the following expectation:
```python
@dlt.expect_or_drop("valid_email", "email IS NULL")
```
What is the effect of this expectation?

A. Records with a null email are dropped; records with a valid email are kept
B. Records with a non-null email are dropped; only records with
   null email are kept — likely dropping all valid records
C. The pipeline fails if any record has a null email
D. Records with null email are flagged but kept in the table

---

**Q5.** Which SQL statement creates a table that drops and recreates
an existing table, losing its version history?

A. `CREATE TABLE IF NOT EXISTS my_table AS SELECT * FROM source`
B. `CREATE TABLE my_table AS SELECT * FROM source`
C. `CREATE OR REPLACE TABLE my_table AS SELECT * FROM source`
D. `INSERT OVERWRITE my_table SELECT * FROM source`

---

**Q6.** A data engineer needs to apply CDC (Change Data Capture) records
to a Silver table — incoming records may represent inserts, updates,
or deletes for existing rows. Which SQL statement is most appropriate?

A. `INSERT INTO silver_table SELECT * FROM cdc_records`
B. `INSERT OVERWRITE silver_table SELECT * FROM cdc_records`
C. `MERGE INTO silver_table USING cdc_records ON ... WHEN MATCHED ... WHEN NOT MATCHED ...`
D. `UPDATE silver_table SET * FROM cdc_records`

---

**Q7.** What does `INSERT OVERWRITE my_table SELECT * FROM source_table`
do by default?

A. Appends new rows to the existing table without affecting existing rows
B. Replaces the entire contents of `my_table` with the result of the SELECT
C. Updates only rows that match on primary key
D. Fails if `my_table` already contains data

---

**Q8.** In a Delta Live Pipeline written in SQL, which keyword prefix is
used to reference another table defined in the same pipeline?

A. `STREAM`
B. `LIVE`
C. `PIPELINE`
D. `DLT`

---

**Q9.** A Gold table aggregates daily revenue by region. A data engineer
wants to add a running 7-day total. Which PySpark construct is appropriate?

A. `df.groupBy("region").rolling(7).sum("revenue")`
B. A window function with `Window.partitionBy("region").orderBy("date")`
   and `rangeBetween` or `rowsBetween` for the 7-day frame
C. `df.withColumn("running_total", df["revenue"].cumsum())`
D. `df.repartition(7).groupBy("region").sum("revenue")`

---

**Q10.** A data engineer runs:
```sql
ALTER TABLE my_table RENAME COLUMN customer_name TO full_name
```
What must be true for this to succeed?

A. The table must be a streaming table
B. Column mapping must be enabled on the table
C. The table must have fewer than 1 million rows
D. The table must be recreated with CREATE OR REPLACE first

---

**Q11.** Which Delta Live Pipeline execution mode runs the pipeline once,
processes all currently available data, and then stops?

A. Continuous
B. Development
C. Triggered
D. Production

---

**Q12.** A Silver table has three expectations defined:
```python
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect_or_drop("non_null_id", "id IS NOT NULL")
@dlt.expect_or_fail("valid_currency", "currency IN ('USD','EUR','GBP')")
```
A batch of records arrives where one record has `currency = 'JPY'`.
What happens?

A. The record with `currency = 'JPY'` is dropped; the pipeline continues
B. The record with `currency = 'JPY'` is flagged but kept; the pipeline continues
C. The entire pipeline run fails due to the `expect_or_fail` violation
D. The record is automatically converted to USD and kept

---

## Answer Key

| Q | Answer | Key Concept |
|---|---|---|
| Q1 | B | Bronze = raw, append-only, faithful copy of source |
| Q2 | C | Single responsibility per layer, independent reprocessing and testing |
| Q3 | B | dlt.read = batch full state; dlt.read_stream = incremental new records |
| Q4 | B | Condition describes good data — "email IS NULL" inverts intent, drops valid records |
| Q5 | C | CREATE OR REPLACE TABLE drops and recreates, losing history |
| Q6 | C | MERGE INTO handles insert/update/delete CDC pattern |
| Q7 | B | INSERT OVERWRITE replaces entire table contents by default |
| Q8 | B | LIVE.table_name references another table in the same DLP pipeline |
| Q9 | B | Window function with rangeBetween/rowsBetween for rolling aggregation |
| Q10 | B | Column mapping must be enabled for RENAME/DROP COLUMN |
| Q11 | C | Triggered mode runs once and stops |
| Q12 | C | expect_or_fail stops the entire pipeline on any violation |