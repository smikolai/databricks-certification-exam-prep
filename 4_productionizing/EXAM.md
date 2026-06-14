# Section 4: Productionizing Data Pipelines — Practice Exam Questions

---

**Q1.** A data engineer wants to run a Delta Live Pipeline nightly at 6am
Mountain Time with an email alert if it fails. Which Databricks feature
handles this?

A. Databricks Asset Bundles — deploy the pipeline to a production environment
B. Lakeflow Jobs — schedule the pipeline with email notifications on failure
C. Delta Live Tables continuous mode — runs the pipeline around the clock
D. Databricks Connect — connects the pipeline to external scheduling tools

---

**Q2.** A job has three tasks: Bronze, Silver, Gold. Silver depends on Bronze.
Gold depends on Silver. Bronze fails after 2 retries. What is the final
state of each task?

A. Bronze: Failed, Silver: Failed, Gold: Failed
B. Bronze: Failed, Silver: Skipped, Gold: Skipped
C. Bronze: Failed, Silver: Running, Gold: Pending
D. Bronze: Failed, Silver: Skipped, Gold: Failed

---

**Q3.** A team maintains separate Databricks workspaces for dev, staging,
and prod. They currently recreate job configurations manually in each
workspace when changes are made. Which tool eliminates this problem?

A. Lakeflow Jobs with multi-workspace scheduling
B. Delta Live Pipelines with environment variables
C. Databricks Asset Bundles — define once in YAML, deploy to any target
D. Databricks Repos — sync notebooks across workspaces automatically

---

**Q4.** A data engineer runs `databricks bundle deploy --target prod`.
What happens?

A. The bundle is validated, deployed to prod, and the job runs immediately
B. The bundle resources are created/updated in the prod workspace
   but the job does NOT run automatically
C. The bundle is destroyed in dev and recreated in prod
D. The command fails — you must run `databricks bundle validate` first

---

**Q5.** What does `mode: development` do in a DAB target configuration?

A. Limits the bundle to read-only operations in the workspace
B. Enables debug logging for all deployed resources
C. Adds a [dev username] prefix to all resource names, preventing
   collision with staging and production resources
D. Deploys resources to a sandbox catalog isolated from production data

---

**Q6.** A job runs for an average of 4 minutes every 30 minutes throughout
the day. Cost efficiency is the primary concern. Which compute type is
most appropriate?

A. All-Purpose Cluster with autoscaling
B. Job Cluster with spot instances
C. Serverless compute — seconds to start, pay only for actual compute used
D. High Concurrency Cluster shared across all job runs

---

**Q7.** Which DAB CLI command checks YAML syntax and resource references
WITHOUT deploying anything?

A. `databricks bundle deploy --dry-run`
B. `databricks bundle validate`
C. `databricks bundle check`
D. `databricks bundle plan`

---

**Q8.** A data engineer needs Task B to run only after Task A completes
successfully. Which YAML configuration achieves this?

A.
```yaml
- task_key: task_b
  after: task_a
```
B.
```yaml
- task_key: task_b
  requires:
    - task_key: task_a
```
C.
```yaml
- task_key: task_b
  depends_on:
    - task_key: task_a
```
D.
```yaml
- task_key: task_b
  run_after: task_a
```

---

**Q9.** What is the key operational difference between a Job Cluster
and Serverless compute for Lakeflow Jobs?

A. Job Clusters support Python notebooks; Serverless only supports SQL
B. Serverless clusters persist between job runs for faster startup;
   Job Clusters terminate after each run
C. Job Clusters spin up for the job and terminate after completion
   with 4-8 minute startup; Serverless starts in seconds with no
   configuration and auto-scales
D. Job Clusters are free; Serverless compute is billed per DBU

---

**Q10.** A data engineer runs `databricks bundle run wine_pipeline_job`.
What must be true for this command to succeed?

A. The bundle must have been previously deployed with
   `databricks bundle deploy`
B. The job must be currently running in the workspace
C. The command validates and deploys the bundle automatically
   before running
D. The Databricks CLI must be version 2.0 or higher

---

**Q11.** Which cron expression runs a job at 6am every Monday?

A. `"0 6 * * MON"`
B. `"0 0 6 ? * MON"`
C. `"6 0 0 * * 1"`
D. `"0 0 6 * * 1-5"`

---

**Q12.** A Lakeflow Job has an ETL Pipeline task followed by a Validation
Notebook task. The ETL Pipeline succeeds but the Validation Notebook
fails. What is the job's final status and what should the engineer
check first?

A. Job: Succeeded — only the pipeline task determines job status
B. Job: Failed — check the Validation Notebook task logs for the
   assertion or code error that caused the failure
C. Job: Partially succeeded — downstream tasks can fail independently
D. Job: Failed — the ETL Pipeline is automatically rolled back

---

## Answer Key

| Q | Answer | Key Concept |
|---|---|---|
| Q1 | B | Lakeflow Jobs handles scheduling and notifications |
| Q2 | B | Failed task → downstream tasks SKIPPED not failed |
| Q3 | C | DAB — define once, deploy to any target |
| Q4 | B | `bundle deploy` deploys resources, does NOT run the job |
| Q5 | C | development mode adds [dev username] prefix to prevent collision |
| Q6 | C | Serverless — seconds startup, pay per use, best for short frequent jobs |
| Q7 | B | `bundle validate` checks without deploying |
| Q8 | C | `depends_on` is the correct YAML key for task dependencies |
| Q9 | C | Job Cluster = 4-8min startup, terminates after; Serverless = seconds, auto-scales |
| Q10 | A | Must deploy before run — deploy and run are separate commands |
| Q11 | B | Quartz cron: seconds minutes hours day month weekday — 6am Mondays |
| Q12 | B | Job fails, check Validation Notebook task logs |