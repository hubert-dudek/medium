# Azure SQL Incremental Migration Monitoring Skill

## Purpose

Monitor and validate the incremental ingestion of `dbo.transactions` from Azure SQL into Databricks, while being aware that a source migration is planned.

The migration to the new Azure SQL server/database is the most important operational context for this skill. The agent must not assume that the current source will remain the active source indefinitely.

---

## Current Databricks Target

- Catalog: `main`
- Schema: `default`
- Target table: `main.default.transactions`
- Incremental key: `transaction_id`
- Source table: `dbo.transactions`
- Incremental strategy: import rows where `transaction_id` is greater than the maximum value already present in `main.default.transactions`

Expected source columns:

- `transaction_id` - `BIGINT`, primary key / identity
- `customer_id` - `INT`
- `transaction_date` - `DATETIME2`
- `amount` - `DECIMAL(18,2)`

---

## Databricks Asset Bundle Job

This ingestion is deployed as a Databricks Asset Bundle job. When monitoring the ingestion, identify the workload by both its bundle resource key and its deployed job/task names.

- Bundle job resource key: `import_job`
- Deployed Databricks job name: `import_job_azure_sql`
- Task key: `import_task`
- Notebook/source path: `../src/import.py`
- Task environment key: `default`
- Serverless environment client version: `5`
- Schedule timezone: `UTC`
- Configured Quartz cron: `0 0 */5 ? * *`

Relevant bundle definition:

```yaml
resources:
  jobs:
    import_job:
      name: import_job_azure_sql
      tasks:
        - task_key: import_task
          notebook_task:
            notebook_path: ../src/import.py
          environment_key: default
      environments:
        - environment_key: default
          spec:
            client: "5"
      schedule:
        quartz_cron_expression: "0 0 */5 ? * *"
        timezone_id: "UTC"
```

### Schedule note

In Quartz cron syntax, `0 0 */5 ? * *` means **every 5 hours at minute 0**, not every 5 minutes. If the intention is every 5 minutes, the expression should be `0 */5 * ? * *`. The agent should report the schedule as configured and should not silently modify it.

When checking job health, monitor the latest run of `import_job_azure_sql` and specifically the `import_task` task. Capture at least run status, start/end time, duration, and failure message when present.

---

## Current Azure SQL Source

Unity Catalog connection:

- Connection name: `azure_sql`
- Server: `server1zero.database.windows.net`
- Port: `1433`
- Database: `free-sql-db-1216115`
- Source table: `dbo.transactions`

The UC connection has already been configured and successfully tested.

Use the Unity Catalog connection rather than manually building a JDBC connection with credentials.

### Connectivity test

```sql
SELECT *
FROM remote_query(
    'azure_sql',
    database => 'free-sql-db-1216115',
    query => 'SELECT TOP 10 * FROM dbo.transactions'
);
```

---

# IMPORTANT: Planned Source Migration

A migration to a new Azure SQL server/database is planned.

The new Unity Catalog connection is already created:

- Connection name: `azure_sql_new`
- Server: `server2zero.database.windows.net`
- Port: `1433`
- JDBC endpoint: `jdbc://server2zero.database.windows.net:1433/`
- New database: `free-sql-db-0536539`
- Expected source table: `dbo.transactions`

Databricks connection page:

`https://dbc-9d54d642-8585.cloud.databricks.com/explore/connections/azure_sql_new?o=7474654043716926`

The agent must treat this migration as an active planned change.

Do not automatically switch production ingestion to `azure_sql_new` merely because the connection exists.

Switch only when the migration has been explicitly confirmed or when the monitoring job is specifically instructed to validate/cut over to the new source.

---

## New Source Validation

Before cutover, validate the new source independently.

### Basic connection test

```sql
SELECT *
FROM remote_query(
    'azure_sql_new',
    database => 'free-sql-db-0536539',
    query => 'SELECT TOP 10 * FROM dbo.transactions'
);
```

### Check source row count

```sql
SELECT *
FROM remote_query(
    'azure_sql_new',
    database => 'free-sql-db-0536539',
    query => 'SELECT COUNT(*) AS row_count FROM dbo.transactions'
);
```

### Check primary-key range

```sql
SELECT *
FROM remote_query(
    'azure_sql_new',
    database => 'free-sql-db-0536539',
    query => '
        SELECT
            MIN(transaction_id) AS min_transaction_id,
            MAX(transaction_id) AS max_transaction_id,
            COUNT(*) AS row_count
        FROM dbo.transactions
    '
);
```

### Check duplicates in the primary key

```sql
SELECT *
FROM remote_query(
    'azure_sql_new',
    database => 'free-sql-db-0536539',
    query => '
        SELECT transaction_id, COUNT(*) AS cnt
        FROM dbo.transactions
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
    '
);
```

Expected result: zero rows.

---

# Incremental Ingestion Logic

The existing ingestion pattern uses the maximum `transaction_id` already loaded to Databricks.

```python
target_table = "main.default.transactions"
connection = "azure_sql"
database = "free-sql-db-1216115"

if spark.catalog.tableExists(target_table):
    last_id = spark.sql(f"""
        SELECT COALESCE(MAX(transaction_id), -1)
        FROM {target_table}
    """).first()[0]
else:
    last_id = -1

df = spark.sql(f"""
    SELECT *
    FROM remote_query(
        '{connection}',
        database => '{database}',
        query => '
            SELECT
                transaction_id,
                customer_id,
                transaction_date,
                amount
            FROM dbo.transactions
            WHERE transaction_id > {last_id}
        '
    )
""")

df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(target_table)
```

This design assumes that:

1. `transaction_id` is unique.
2. New records receive monotonically increasing IDs.
3. Existing rows are not expected to be updated after ingestion.
4. Deletes from Azure SQL do not need to be propagated.

If any of these assumptions change, the ingestion strategy must be reviewed.

---

# Monitoring Responsibilities

The monitoring agent should check the following.

## 0. Databricks job/task state

Monitor the bundle-deployed ingestion workload:

- job: `import_job_azure_sql`
- bundle resource key: `import_job`
- task: `import_task`
- source: `../src/import.py`

For the most recent run, capture:

- job run status
- `import_task` status
- start and end time
- duration
- failure/error message, if any
- whether a scheduled run was skipped, cancelled, or still running

A successful UC connection test does not prove that the scheduled job is healthy; job/task execution must be checked separately.

## 1. Databricks target state

Capture:

```sql
SELECT
    COUNT(*) AS row_count,
    MIN(transaction_id) AS min_transaction_id,
    MAX(transaction_id) AS max_transaction_id,
    MAX(transaction_date) AS latest_transaction_date
FROM main.default.transactions;
```

Track at minimum:

- current maximum `transaction_id`
- total row count
- most recent `transaction_date`
- time of the latest successful ingestion

---

## 2. Active Azure SQL source state

Before migration, the active source is:

- connection: `azure_sql`
- database: `free-sql-db-1216115`

Check:

- connection succeeds
- `dbo.transactions` exists
- maximum `transaction_id`
- row count
- whether records exist above the Databricks high-water mark

Example:

```python
last_id = spark.sql("""
    SELECT COALESCE(MAX(transaction_id), -1)
    FROM main.default.transactions
""").first()[0]

pending = spark.sql(f"""
    SELECT *
    FROM remote_query(
        'azure_sql',
        database => 'free-sql-db-1216115',
        query => '
            SELECT COUNT(*) AS pending_rows
            FROM dbo.transactions
            WHERE transaction_id > {last_id}
        '
    )
""")

display(pending)
```

---

## 3. Planned new Azure SQL source

Periodically validate that the planned destination/source is reachable:

- connection: `azure_sql_new`
- database: `free-sql-db-0536539`

Do not treat reachability alone as migration completion.

The agent should report separately:

- new connection reachable: yes/no
- new database reachable: yes/no
- `dbo.transactions` exists: yes/no
- source row count
- min/max transaction ID
- whether its data appears consistent with the current source

---

# Migration Readiness Comparison

Before cutover, compare the two Azure SQL sources.

Useful checks:

- schema matches
- row counts are expected
- primary key ranges are expected
- no duplicate `transaction_id`
- latest transactions are present on the new server
- no unexplained gap exists between the old and new source

Example comparison queries:

```sql
-- Current source
SELECT *
FROM remote_query(
    'azure_sql',
    database => 'free-sql-db-1216115',
    query => '
        SELECT
            COUNT(*) AS row_count,
            MIN(transaction_id) AS min_id,
            MAX(transaction_id) AS max_id
        FROM dbo.transactions
    '
);
```

```sql
-- Planned new source
SELECT *
FROM remote_query(
    'azure_sql_new',
    database => 'free-sql-db-0536539',
    query => '
        SELECT
            COUNT(*) AS row_count,
            MIN(transaction_id) AS min_id,
            MAX(transaction_id) AS max_id
        FROM dbo.transactions
    '
);
```

The agent must not declare migration readiness solely because row counts match. Validate key ranges and recent records as well.

---

# Cutover Procedure

When migration is explicitly approved:

1. Stop or pause ingestion from `azure_sql`.
2. Record the final high-water mark from the old source:
   - `MAX(transaction_id)`
   - row count
3. Validate that the new source contains the required data through that high-water mark.
4. Run a test query through `azure_sql_new`.
5. Change ingestion configuration to:

```python
connection = "azure_sql_new"
database = "free-sql-db-0536539"
```

6. Keep the target unchanged:

```text
main.default.transactions
```

7. Continue incremental ingestion using the existing Databricks high-water mark.
8. Verify that the first post-cutover batch does not introduce duplicate IDs or unexpected gaps.
9. Report the migration/cutover result.

---

# Important Cutover Warning

The current demo/test setup has used different identity starting values in different source versions, for example:

- one source/version starting around `transaction_id = 0`
- another source/version starting around `transaction_id = 1000`

Therefore a gap in IDs is not automatically an error.

For example:

```text
0
1
2
3
1000
1001
1002
```

can be valid if that is how the migration/test data was intentionally created.

The agent should distinguish between:

- an intentional identity jump
- missing data caused by a migration problem

Do not infer data loss from a non-contiguous ID sequence without checking the migration plan/source data.

---

# Unity Catalog Connection Usage

Preferred connectivity method:

```sql
remote_query(...)
```

Use UC connections:

```text
azure_sql
azure_sql_new
```

Do not place Azure SQL passwords directly in notebook code.

A Unity Catalog secret was also tested earlier using the new UC secrets capability:

```python
dbutils.secrets.get(
    catalog="main",
    schema="default",
    key="import"
)
```

However, for this ingestion workflow the existing Unity Catalog SQL connections are preferred because they centralize the Azure SQL connection configuration and credentials.

---

# Azure SQL Networking Context

Azure SQL server-level firewall rules were configured for Databricks AWS `us-west-2` outbound addresses.

Relevant ranges documented during setup:

```text
18.246.106.0   - 18.246.106.255
3.42.138.0     - 3.42.138.127
44.234.192.32  - 44.234.192.47
52.27.216.188  - 52.27.216.188
```

An additional range was also added:

```text
18.98.3.224 - 18.98.3.239
```

That range was identified as an inbound Databricks range and is not required for the Azure SQL outbound connection.

If connectivity fails after migration, verify that the new Azure SQL server `server2zero.database.windows.net` has the appropriate firewall/network configuration as well. Firewall configuration on `server1zero` does not automatically apply to `server2zero`.

---

# Failure Interpretation

When ingestion fails, distinguish between source connectivity and Delta write behavior.

Spark evaluation can be lazy, so a source connection failure may only surface when an action occurs, for example:

```python
df.write.saveAsTable(...)
```

This does not necessarily mean that the Delta write itself is the problem.

For troubleshooting, test the source directly with:

```sql
SELECT *
FROM remote_query(
    '<connection>',
    database => '<database>',
    query => 'SELECT TOP 1 * FROM dbo.transactions'
);
```

Typical failure categories:

- connection timeout / refused -> network or firewall
- login/authentication failure -> connection credentials/auth configuration
- cannot open database -> wrong database name or missing permissions
- table/object not found -> wrong database/schema/table
- Databricks egress denied -> serverless network policy
- Delta duplicate/unexpected data -> ingestion/cutover logic

---

# Agent Decision Rules

The agent MUST:

- identify the ingestion workload as Databricks job `import_job_azure_sql`, bundle resource `import_job`, task `import_task`
- treat `azure_sql` / `free-sql-db-1216115` as the current source until cutover is confirmed
- remember that migration to `azure_sql_new` / `free-sql-db-0536539` is planned
- validate the new source without silently switching ingestion
- use `transaction_id` as the high-water mark
- preserve `main.default.transactions` as the Databricks target
- prefer Unity Catalog connections and `remote_query`
- clearly report which Azure SQL connection/database was checked
- clearly distinguish migration readiness from migration completion
- report suspicious gaps, duplicates, schema differences, or lag
- avoid exposing credentials

The agent MUST NOT:

- automatically cut over merely because `azure_sql_new` is reachable
- recreate connections or rotate credentials unless explicitly requested
- delete or truncate `main.default.transactions`
- reset the high-water mark without explicit instruction
- assume an ID gap means lost records
- use the old server's firewall configuration as proof that the new server is reachable

---

# Suggested Monitoring Output

A concise monitoring report should contain:

```text
Azure SQL migration monitoring

Job:
  name: import_job_azure_sql
  bundle resource: import_job
  task: import_task
  latest run: SUCCESS / FAILED / RUNNING / NOT FOUND
  schedule: 0 0 */5 ? * * (UTC)

Active source:
  connection: azure_sql
  database: free-sql-db-1216115
  status: OK / FAILED

Databricks:
  target: main.default.transactions
  max transaction_id: ...
  row count: ...

Pending source rows:
  ...

Planned new source:
  connection: azure_sql_new
  database: free-sql-db-0536539
  status: READY / NOT READY / NOT CHECKED
  max transaction_id: ...
  row count: ...

Migration:
  status: PLANNED / READY FOR CUTOVER / CUTOVER CONFIRMED / ISSUE
  notes: ...
```

---

# Current Migration State

As of the creation of this skill:

```text
Databricks job:
  bundle resource: import_job
  job name: import_job_azure_sql
  task: import_task
  source: ../src/import.py
  schedule: 0 0 */5 ? * * UTC

Current source:
  azure_sql
  server1zero.database.windows.net:1433
  free-sql-db-1216115

Planned new source:
  azure_sql_new
  server2zero.database.windows.net:1433
  free-sql-db-0536539

Target:
  main.default.transactions

Migration status:
  PLANNED
```
