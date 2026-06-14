# Section 4: Productionizing Data Pipelines

## What Productionizing Means

Moving a pipeline from development to production means:

```
Development:              Production:
──────────────────────────────────────────────────
Run manually              Runs on schedule automatically
Lives in a notebook       Lives in version-controlled code
No retry logic            Automatic retry on failure
You monitor it            Alerts fire when it breaks
One environment           Dev / staging / prod environments
```

Databricks provides two tools:

```
Lakeflow Jobs       ──► orchestration
                        schedule and run pipelines/notebooks/scripts
                        think: Airflow built into Databricks

Databricks Asset    ──► CI/CD
Bundles (DAB)           package, version, deploy resources across environments
                        think: Terraform for Databricks resources
```

---

## Lakeflow Jobs

A Job is a scheduled, orchestrated set of tasks.

### Task Types

```
Notebook          ──► run a Databricks notebook
ETL Pipeline      ──► run a Delta Live / Lakeflow Declarative Pipeline
Python script     ──► run a .py file
SQL               ──► run a SQL query or file
dbt               ──► run a dbt project
Spark submit      ──► run a JAR or Python via spark-submit
```

### Task Dependencies

Tasks form a DAG — downstream tasks only run if upstream tasks succeed:

```
Bronze Ingestion (ETL Pipeline)
        │
        ▼
Silver Transform (Notebook)
        │
        ├──► Gold Aggregation (Notebook)
        └──► Data Quality Report (Notebook)
```

```yaml
tasks:
  - task_key: bronze_ingestion
    pipeline_task:
      pipeline_id: 12345

  - task_key: silver_transform
    notebook_task:
      notebook_path: /pipelines/silver
    depends_on:
      - task_key: bronze_ingestion

  - task_key: gold_aggregation
    notebook_task:
      notebook_path: /pipelines/gold
    depends_on:
      - task_key: silver_transform
```

### Failure Behavior

```
Task fails:
  ──► Task marked FAILED
  ──► All downstream dependent tasks SKIPPED
  ──► Job marked FAILED
  ──► Alerts fire (if configured)
  ──► Upstream completed tasks unaffected

Task retry:
  Configurable per task
  Default: 0 retries
  Max retries: set per task
  Retry on timeout: optional
```

### Scheduling

```yaml
schedule:
  quartz_cron_expression: "0 0 6 * * ?"   # 6am daily
  timezone_id: "America/Denver"            # Mountain Time, DST-aware
```

Quartz cron format: `seconds minutes hours day month weekday`

```
"0 0 6 * * ?"    ──► 6:00am every day
"0 0 */6 * * ?"  ──► every 6 hours
"0 0 8 ? * MON"  ──► 8am every Monday
```

### Compute for Jobs

```
Job Cluster (traditional):
  Defined in job configuration
  Spins up when job starts
  Terminates when job ends
  You configure instance types and worker count
  4-8 minute spin-up time
  Cost: pay for full cluster duration

Serverless:
  No cluster configuration
  Databricks manages infrastructure
  Seconds to start
  Auto-scales automatically
  Cost: pay only for actual compute used
  Best for: short jobs, variable workloads, cost optimization
```

```yaml
# Job cluster configuration
job_clusters:
  - job_cluster_key: main_cluster
    new_cluster:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2

tasks:
  - task_key: my_task
    job_cluster_key: main_cluster   # references the cluster above

# Serverless — no job_cluster_key needed
tasks:
  - task_key: my_task
    environment_key: serverless
```

### Email Notifications

```yaml
email_notifications:
  on_failure:
    - engineer@company.com
  on_success:
    - team@company.com
  on_start:
    - monitor@company.com
```

---

## Databricks Asset Bundles (DAB)

DAB is infrastructure-as-code for Databricks resources.
Define jobs, pipelines, and clusters in YAML. Deploy with CLI commands.

### Why DAB

```
Without DAB:
  Create job in dev UI
  Recreate in staging UI
  Recreate in prod UI
  Any change = update in three places
  No version control of configuration
  No review process for changes

With DAB:
  Define once in YAML
  Deploy anywhere with one command
  Config lives in Git alongside code
  Changes go through PR review
  Rollback = git revert + redeploy
```

### Project Structure

```
my_pipeline/
├── databricks.yml          ──► main bundle config
├── resources/
│   ├── wine_job.yml        ──► job definition
│   └── wine_pipeline.yml   ──► DLT pipeline definition
├── src/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
└── .databricks/
    └── bundle.yml          ──► local state (gitignore this)
```

### databricks.yml Structure

```yaml
bundle:
  name: wine_quality_pipeline

workspace:
  host: https://my-workspace.azuredatabricks.net

targets:
  dev:
    mode: development      # adds [dev username] prefix to resource names
    default: true          # used when no --target specified
    workspace:
      root_path: /Users/${workspace.current_user.userName}/dev

  staging:
    workspace:
      root_path: /Shared/staging

  prod:
    mode: production       # exact names, no prefix
    workspace:
      root_path: /Shared/prod

resources:
  jobs:
    wine_pipeline_job:
      name: Wine Quality Pipeline Job
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "America/Denver"
      email_notifications:
        on_failure:
          - engineer@company.com
      tasks:
        - task_key: run_etl_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.wine_etl.id}

        - task_key: validate_output
          notebook_task:
            notebook_path: ./src/wine_validation.py
          depends_on:
            - task_key: run_etl_pipeline

  pipelines:
    wine_etl:
      name: Wine ETL Pipeline
      target: workspace.wine_dlp
      libraries:
        - notebook:
            path: ./src/wine_dlp_pipeline.py
```

### Development vs Production Mode

```
mode: development
  Resource names prefixed with [dev username]
  e.g. "Wine Quality Pipeline" becomes "[dev scott] Wine Quality Pipeline"
  Prevents dev resources colliding with production
  Useful for multiple engineers working simultaneously

mode: production
  Exact names as defined — no prefix
  Production monitoring and alerts enabled
  Stricter validation at deploy time
```

### DAB CLI Commands

```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate
databricks configure --token
# Prompts for: workspace URL, personal access token

# Validate bundle — checks YAML syntax and resource references
databricks bundle validate

# Deploy to default target (dev)
databricks bundle deploy

# Deploy to specific target
databricks bundle deploy --target staging
databricks bundle deploy --target prod

# Run a job defined in the bundle
databricks bundle run wine_pipeline_job

# Run with specific target
databricks bundle run wine_pipeline_job --target prod

# Tear down all deployed resources
databricks bundle destroy
databricks bundle destroy --target prod
```

### Variable Substitution in DAB

```yaml
# Reference another resource's ID
pipeline_id: ${resources.pipelines.wine_etl.id}

# Reference current user
root_path: /Users/${workspace.current_user.userName}

# Define custom variables
variables:
  env:
    default: dev
    description: Deployment environment

# Use custom variable
name: Wine Pipeline ${var.env}
```

---

## Spark UI for Job Monitoring

The exam tests knowing where to find specific information in the Spark UI
when a job is running or has completed.

```
Jobs tab        ──► which jobs ran, duration, status
Stages tab      ──► task duration distribution, data skew
Executors tab   ──► memory usage, GC time, shuffle bytes
SQL tab         ──► physical plan, DAG visualization
Storage tab     ──► cached DataFrames and sizes
```

For a running Lakeflow Job:
```
Jobs & Pipelines ──► click the job run
                 ──► click into a task
                 ──► click "Spark UI" link
                 ──► full Spark UI for that task's cluster
```

---

## Exam Decision Tree

```
Need to schedule a pipeline to run nightly?
  ──► Lakeflow Jobs

Need to deploy same pipeline to dev/staging/prod?
  ──► Databricks Asset Bundles

Need Task B to wait for Task A?
  ──► depends_on in job task configuration

Task A fails — what happens to Task B?
  ──► Task B is SKIPPED (not failed)

Job runs for 3 minutes every hour, cost is priority?
  ──► Serverless compute

Need email alert when job fails?
  ──► email_notifications.on_failure in job config

Want to prevent dev resources colliding with prod?
  ──► DAB mode: development adds [dev username] prefix
```

---

## Exam Traps

- Task failure causes downstream tasks to be **skipped**, not failed
- DAB `mode: development` adds username prefix to ALL resource names
  automatically — prevents dev/prod collision
- Serverless has no cluster configuration — Databricks manages it entirely
- `databricks bundle validate` checks syntax only — does not deploy
- `databricks bundle deploy` deploys but does NOT run the job
- `databricks bundle run` runs the job after deploying
- Quartz cron in DAB: seconds field is first — different from Unix cron
- Job clusters terminate after job completion — cannot attach a notebook
  to a job cluster interactively