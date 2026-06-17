# Section 2: Development and Ingestion

## Databricks Notebooks

Notebooks are the primary development environment in Databricks.
They support Python, SQL, Scala, and R — and can mix languages within
a single notebook using magic commands.

### Magic Commands

Magic commands extend notebook functionality beyond the default language.
Prefixed with `%` (line magic) or `%%` (cell magic).

```
%python     ──► run cell as Python
%sql        ──► run cell as SQL
%scala      ──► run cell as Scala
%r          ──► run cell as R
%md         ──► render cell as Markdown documentation
%sh         ──► run shell commands on driver node
%fs         ──► interact with DBFS (Databricks File System)
%run        ──► execute another notebook inline (must be in its own cell)
%pip        ──► install Python packages in notebook session
%reload     ──► reload a Python module
```

**Key exam facts about magic commands:**
- Cell magic (`%%`) must be the first line of the cell
- A cell can only have ONE magic command
- Variables are isolated between language REPLs — Python variables
  are NOT accessible in `%sql` or `%scala` cells
- `%run` runs the entire target notebook inline — all its variables
  and functions become available in the calling notebook
- SQL cell results are automatically assigned to `_sqldf` as a DataFrame

```python
# After a %sql cell runs, result is available as _sqldf
# In the next Python cell:
display(_sqldf.filter(col("amount") > 1000))
```

### Notebook Widgets

Widgets allow parameterized notebooks — inputs that can be set
at runtime by a job, another notebook, or a user.

```python
# Create a text widget
dbutils.widgets.text("environment", "dev", "Environment")

# Create a dropdown widget
dbutils.widgets.dropdown("table_name", "customers",
    ["customers", "orders", "products"], "Table Name")

# Read widget value
env = dbutils.widgets.get("environment")
table = dbutils.widgets.get("table_name")

# Remove widgets
dbutils.widgets.remove("environment")
dbutils.widgets.removeAll()
```

### dbutils

`dbutils` is the Databricks utility library. Key modules:

```python
# File system operations
dbutils.fs.ls("/mnt/data")           # list files
dbutils.fs.cp("/src", "/dst")        # copy
dbutils.fs.mv("/src", "/dst")        # move
dbutils.fs.rm("/path", recurse=True) # delete

# Secrets — retrieve secrets from secret scope
dbutils.secrets.get(scope="my-scope", key="db-password")

# Notebook utilities
dbutils.notebook.run("/path/to/notebook", timeout_seconds=300,
                     arguments={"env": "prod"})
dbutils.notebook.exit("success")     # exit with return value

# Widgets (see above)
dbutils.widgets.text(...)
dbutils.widgets.get(...)
```

### Debugging Tools

```python
# Display DataFrame with rich formatting
display(df)

# Print schema
df.printSchema()

# Show execution plan
df.explain(True)   # True = extended plan with stats

# Databricks built-in debugger
# Available for Python notebooks
# Set breakpoints, inspect variables, step through code
# Activated via the "Bug" icon in the notebook toolbar
```

---

## Databricks Connect

Databricks Connect allows local IDE development (VS Code, PyCharm, IntelliJ)
connected to a remote Databricks cluster. Write and test code locally,
execute on the cluster.

```python
# Install
pip install databricks-connect

# Configure
databricks-connect configure
# Prompts for: host, token, cluster ID, org ID, port

# Use in local code — identical to notebook code
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# This SparkSession connects to the remote Databricks cluster

df = spark.read.table("catalog.schema.my_table")
df.show()
```

**Key facts:**
- Databricks Connect version must match the Databricks Runtime version on the cluster
- Supports Python only (not Scala/R via local IDE)
- Runs DataFrame operations on the remote cluster
- Local machine handles Python driver logic only
- Does NOT support `dbutils` fully in local mode
- Replaced older `databricks-connect` with the newer gRPC-based version in DBR 13+

**Use cases:**
- Local unit testing of transformation logic
- IDE features (autocomplete, refactoring) for Spark code
- CI/CD pipelines that run tests against a real cluster

---

## Auto Loader

Auto Loader is Databricks' incremental file ingestion framework.
It monitors cloud storage for new files and processes them exactly once
using Structured Streaming under the hood.

### Why Auto Loader Over Standard readStream

```
Standard spark.readStream.format("parquet").load("/path"):
  Lists the entire directory every trigger
  Slow at scale — millions of files = slow listing
  No schema inference or evolution
  No rescue of malformed records

Auto Loader (cloudFiles):
  Two detection modes — directory listing or file notifications
  Efficient at scale — file notification mode uses cloud events
  Automatic schema inference and evolution
  _rescued_data column captures malformed records
  Exactly-once processing via checkpoint
```

### Basic Auto Loader Syntax

```python
# The format is always "cloudFiles"
# cloudFiles.format specifies the actual file format

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")          # actual file format
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("/mnt/landing/json-files/")
)

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/my_table/") \
    .trigger(availableNow=True) \
    .toTable("catalog.schema.my_table")
```

**Supported formats for `cloudFiles.format`:**
```
json, csv, parquet, avro, orc, text, binaryFile
```

### Key Auto Loader Options

```python
# File format
.option("cloudFiles.format", "json")

# Schema location — required for schema inference/evolution
# Can be same directory as checkpointLocation
.option("cloudFiles.schemaLocation", "/path/to/schema/")

# File detection mode
.option("cloudFiles.useNotifications", "false")  # default: directory listing
.option("cloudFiles.useNotifications", "true")   # file notification mode

# Schema hints — provide type hints while still inferring rest
.option("cloudFiles.schemaHints", "id INT, amount DOUBLE")

# includeExistingFiles — controls whether files that existed in the
# source directory BEFORE the stream first started are processed.
# Default: true. Set false to only process files arriving from
# stream start onward (skip the existing backlog).
.option("cloudFiles.includeExistingFiles", "true")

# Max files per trigger — rate limiting
.option("cloudFiles.maxFilesPerTrigger", "1000")

# Path glob filter — only process matching files
.option("pathGlobFilter", "*.json")
```

### File Metadata Column

Every DataFrame read from files (not just Auto Loader) can access a
built-in `_metadata` column containing per-row file provenance —
useful for auditing which source file each row came from.

```python
from pyspark.sql.functions import col

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(my_schema)
    .load("/mnt/landing/")
    .select("*", "_metadata")   # add the metadata struct column
)

# _metadata fields include:
#   _metadata.file_path
#   _metadata.file_name
#   _metadata.file_size
#   _metadata.file_modification_time

df.select(col("_metadata.file_path"), col("_metadata.file_name"))
```

### File Detection Modes

**Directory Listing (default):**
```
Auto Loader lists the directory and tracks which files have been processed
via the checkpoint. New files detected by comparing current listing
against checkpoint state.

Best for:  lower file volumes, simpler setup, no cloud permissions needed
Weakness:  slower at scale — listing millions of files takes time
```

**File Notification Mode:**
```
Auto Loader sets up cloud event notifications (S3 Event Notifications,
Azure Event Grid, GCS Pub/Sub) to receive events when new files land.
No directory listing needed.

Best for:  high file volumes, near-real-time ingestion
Requires:  cloud permissions to set up notification services
```

### Schema Inference and Evolution

Auto Loader can infer schema from sampled files and evolve it
as new columns appear:

```python
# Schema inference — Auto Loader samples files and infers schema
# schemaLocation persists the inferred schema across restarts
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("/mnt/landing/")
)
```

**Schema evolution modes:**
```python
# addNewColumns (default) — new columns added, query restarts automatically
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

# rescue — new/mismatched columns go to _rescued_data column, no restart
.option("cloudFiles.schemaEvolutionMode", "rescue")

# failOnNewColumns — throws exception if new column detected
.option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")

# none — ignores new columns
.option("cloudFiles.schemaEvolutionMode", "none")
```

### _rescued_data Column

Auto Loader automatically adds a `_rescued_data` column that captures:
- Columns not in the schema
- Data that couldn't be parsed
- Type mismatches

```python
# Rows with extra/malformed fields have them captured here
# instead of failing the entire stream
df.filter(col("_rescued_data").isNotNull()).show()
```

### Auto Loader with availableNow Trigger

```python
# Process all currently available files then stop
# Equivalent to a batch job that catches up incrementally
df.writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/checkpoints/") \
    .toTable("my_table")
```

Useful for scheduled batch pipelines that want incremental processing
without a continuously running stream.

### Auto Loader in Delta Live Pipelines

When used inside a Delta Live Pipeline (LDP), checkpoint and schema
locations are managed automatically — no need to specify them:

```sql
CREATE OR REFRESH STREAMING TABLE raw_events
AS SELECT * FROM STREAM read_files(
    '/mnt/landing/events/',
    format => 'json'
)
```

---

## Valid Auto Loader Sources

The exam tests which sources are valid for Auto Loader:

```
✅ Valid sources:
  Cloud object storage:
    AWS S3
    Azure Data Lake Storage (ADLS) Gen2
    Google Cloud Storage (GCS)
  Unity Catalog Volumes
  DBFS paths (backed by cloud storage)

❌ NOT valid sources:
  Relational databases (use JDBC instead)
  Kafka topics (use Structured Streaming kafka source)
  Delta tables (use spark.readStream.format("delta"))
  REST APIs
```

**Key distinction:** Auto Loader is for **files in cloud object storage**.
For anything else, use the appropriate Structured Streaming source.

---

## Exam Traps

- Auto Loader format is always `"cloudFiles"` — `cloudFiles.format` is the actual file format
- `cloudFiles.schemaLocation` is required for schema inference — without it, you must provide schema explicitly
- Variables do NOT cross language boundaries in notebooks — Python variables unavailable in `%sql` cells
- `%run` must be in its own cell — cannot be combined with other code
- SQL cell results auto-assign to `_sqldf` — accessible in subsequent Python cells
- Auto Loader is for cloud object storage files only — not Kafka, not databases
- `availableNow=True` trigger processes all current files then stops — useful for scheduled incremental batch
- File notification mode requires cloud permissions to set up event infrastructure
- `_rescued_data` captures malformed/extra columns — does not fail the stream