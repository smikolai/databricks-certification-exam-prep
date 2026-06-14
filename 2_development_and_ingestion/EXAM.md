# Section 2: Development and Ingestion — Practice Exam Questions

---

**Q1.** A data engineer wants to run SQL in a Python notebook cell without
switching the notebook's default language. Which magic command do they use?

A. `%%sql`
B. `%spark.sql`
C. `%sql`
D. `#sql`

---

**Q2.** A data engineer runs a `%sql` cell that queries a Delta table.
They then want to filter the results using PySpark in the next cell.
How do they access the SQL result as a DataFrame?

A. `df = spark.sql("SELECT * FROM my_table")`
B. `df = _sqldf`
C. `df = dbutils.notebook.getResult()`
D. `df = spark.table("_sql_result")`

---

**Q3.** Which of the following is a valid source for Auto Loader?

A. A Kafka topic receiving JSON events
B. A PostgreSQL table updated every 5 minutes
C. A Delta table receiving streaming updates
D. An S3 bucket receiving JSON files from an IoT device

---

**Q4.** A data engineer writes the following Auto Loader code:
```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/") \
    .load("/mnt/landing/")
```
What does `cloudFiles.schemaLocation` do?

A. Specifies where processed files are archived after ingestion
B. Persists the inferred schema across stream restarts enabling
   schema inference and evolution tracking
C. Sets the location where Auto Loader writes output Delta files
D. Defines the cloud storage bucket for file notification events

---

**Q5.** A new column `discount_pct` appears in incoming JSON files that
was not in the original schema. Auto Loader is configured with default
schema evolution mode. What happens?

A. The stream fails with a schema mismatch error
B. The new column is silently dropped from all records
C. The new column is captured in `_rescued_data` and the stream continues
D. The new column is added to the schema and the stream restarts automatically

---

**Q6.** A data engineer needs to process all files currently in an S3 bucket
that have not yet been loaded, then stop — like an incremental batch job.
Which trigger configuration achieves this?

A. `.trigger(Trigger.Once())`
B. `.trigger(availableNow=True)`
C. `.trigger(Trigger.ProcessingTime("0 seconds"))`
D. `.trigger(Trigger.Continuous("1 second"))`

---

**Q7.** What is the key advantage of Auto Loader's file notification mode
over directory listing mode?

A. File notification mode supports more file formats than directory listing
B. File notification mode uses cloud event notifications, avoiding
   directory listing at scale — more efficient for high file volumes
C. File notification mode automatically infers schema without
   requiring a schemaLocation
D. File notification mode works with Kafka topics in addition to
   cloud object storage

---

**Q8.** A data engineer runs `%run /Shared/utilities/helpers` in a notebook.
What happens?

A. The helpers notebook is imported as a Python module
B. The helpers notebook executes inline and its variables and functions
   become available in the calling notebook
C. A new tab opens with the helpers notebook running independently
D. The helpers notebook is scheduled to run after the current notebook completes

---

**Q9.** Which statement about language magic commands in Databricks notebooks
is correct?

A. Variables defined in a Python cell are accessible in a subsequent `%sql` cell
B. A cell can contain multiple magic commands as long as they are on separate lines
C. Variables and state are isolated between different language REPLs —
   Python variables are not accessible in `%scala` or `%sql` cells
D. `%%python` and `%python` are identical in behavior

---

**Q10.** A data engineer configures Auto Loader to ingest CSV files from ADLS Gen2.
New files arrive at high volume — thousands per minute. Which detection mode
should they use and why?

A. Directory listing — simpler setup, no additional cloud permissions needed
B. File notification mode — uses Azure Event Grid to receive file arrival
   events, avoiding slow directory listing at high file volumes
C. Directory listing with `cloudFiles.maxFilesPerTrigger` set to 1
D. File notification mode — required for all CSV file ingestion regardless
   of volume

---

**Q11.** What does Databricks Connect enable that is NOT possible with
standard Spark development?

A. Faster query execution by running code directly on the driver
B. Local IDE development (VS Code, PyCharm) connected to a remote
   Databricks cluster — write and debug locally, execute on cluster
C. Real-time collaboration between multiple engineers on the same notebook
D. Automatic deployment of notebooks to production clusters

---

**Q12.** A data engineer uses `dbutils.secrets.get(scope="prod", key="db-pass")`
in a notebook. What does this return?

A. The secret value as a string — usable in connection strings and configs
B. A masked string that prints as `[REDACTED]` but works in API calls
C. A reference object that must be passed directly to connector functions
D. The secret metadata including creation date and last accessed time

---

## Answer Key

| Q | Answer | Key Concept |
|---|---|---|
| Q1 | C | `%sql` runs a single cell as SQL in a Python notebook |
| Q2 | B | SQL cell results auto-assigned to `_sqldf` |
| Q3 | D | Auto Loader is for cloud object storage files — not Kafka, not databases |
| Q4 | B | schemaLocation persists inferred schema for evolution tracking |
| Q5 | D | Default addNewColumns mode adds column and restarts stream |
| Q6 | B | availableNow=True processes all current files then stops |
| Q7 | B | File notification avoids directory listing — efficient at high volume |
| Q8 | B | %run executes inline — variables and functions available in calling notebook |
| Q9 | C | Language REPLs are isolated — variables don't cross boundaries |
| Q10 | B | High volume → file notification mode via Azure Event Grid |
| Q11 | B | Databricks Connect = local IDE connected to remote cluster |
| Q12 | A | dbutils.secrets.get returns the actual secret value as a string |
