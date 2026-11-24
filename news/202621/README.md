# Databricks News · recording guide

The 26 chapters follow the published article in order. Use **AWS-DEV** throughout, and switch to **AZURE-MVPLINK** for chapter 11. Chapters 24 and 25 use the Databricks **AWS account** that contains AWS-DEV. [Medium article](https://medium.com/databrickscommunity/databricks-news-genie-cli-app-builder-agent-memory-dabs-41820427fcc2)

## Before recording

1. Open your `medium` Git folder in each workspace and browse to `news/202621`. The two-digit folder names match the table. On AWS-DEV, use **test_warehouse** for SQL and **serverless environment 6** for Python notebooks. Open notebooks from the Git folder so their sibling data files are available.
2. Open PowerShell in the local checkout. Run the following once for this terminal, then change into the numbered chapter folder before running its command.

   ```powershell
   $env:Path = "C:\git\cli;C:\git\temp\news-202621-tools\unity-gateway\Scripts;$env:Path"
   $env:DATABRICKS_CONFIG_FILE = 'C:\git\cli\.databrickscfg'
   Set-Location C:\git\medium\news\202621
   $warehouse = (databricks warehouses list -p AWS-DEV -o json | ConvertFrom-Json | Where-Object name -eq 'test_warehouse').id
   ```

3. If you want to show the Azure ASOF result live, deploy the bundle in chapter 11 shortly before that segment. Startup takes longer than the query. The two AWS jobs in chapters 5 and 12 are on demand. You can open their existing runs during the recording.

## Development

| # | Article subtitle and folder | Workspace | What to show |
|---|---|---|---|
| 01 | [DABs deployment history](01_deployment_history/) | AWS-DEV · CLI | Run `.\history.ps1`. Show the two successful versions of the article’s bundle, who deployed them, and the managed job. |
| 02 | [Data quality monitors in DABs](02_data_quality_monitors/) | AWS-DEV · CLI | Run `.\preview.ps1 -UseSeedData`. The four-row sales table already exists. Point to the table snapshot and schema anomaly detection in the resolved configuration. The source has three non-null amounts averaging 20. |
| 03 | [Feature materialization in DABs](03_feature_materialization/) | AWS-DEV · SQL + bundle | Open `databricks.yml` at `txn_count` and follow its reference into `customer_features`. Run the first three cells of `demo.sql` against the prepared transactions. The seven-day reference query returns counts of 2 and 1. The last cell lists actual managed output tables if a materialization has run. |
| 04 | [Wait for permissions in DABs](04_wait_for_permissions/) | AWS-DEV · CLI | Open `databricks.yml` and run `databricks bundle validate -p AWS-DEV`. Trace the consumer job’s dependency to `jobs.source.permissions` and show which identity owns the source job. |
| 05 | [AI Function tasks in DABs](05_ai_function_tasks/) | AWS-DEV · Jobs + SQL | Open **news_202621_classify_tickets** in Jobs, then show `ai_function_task` in the YAML and run `results.sql`. To replay, run `databricks bundle run classify_tickets -p AWS-DEV`. The completed run and both labels (billing and technical) are ready to show. |
| 06 | [Sensitive variables in DABs](06_sensitive_variables/) | AWS-DEV · CLI | Run `.\run.ps1`. Show explicit redaction, automatic redaction for a UC secret, and the CLI rejecting the same sample value in a job parameter. |
| 07 | [Shared Serverless for Git folders and DABs](07_shared_serverless/) | AWS-DEV · bundle editor | At **07_shared_serverless**, choose **Open in editor** so this folder is the bundle root. Open `pyproject.toml`, apply the environment, and select **Shared Serverless** in both notebooks under the same usage policy. Run `first.py` then `second.py`. Both import the pinned package. The second prints `False` for the variable created in the first. |
| 08 | [Genie Code CLI](08_genie_cli/) | AWS-DEV · local terminal | Run `.\run.ps1`. The script sends the two local YAML files to Genie over standard input. Genie explains the job timeout change from 60 to 120 seconds. Model usage goes through Unity Gateway. |
| 09 | [Genie App Builder](09_genie_app_builder/) | AWS-DEV · Apps | Open **databricks-news-tracker** in App Space **space** and launch its **Genie App Builder** preview. Select **AI/ML** to take the list from six cards to two. Use `prompt.txt` to recreate it. The original source is here too. For a local preview, run `pnpm install --frozen-lockfile` on a fresh checkout, then `pnpm run build:client` and `C:\git\temp\news-202621-spark43-venv\Scripts\python.exe -m http.server 8765 --directory client/dist` and open localhost port 8765. |

## Data engineering

| # | Article subtitle and folder | Workspace | What to show |
|---|---|---|---|
| 10 | [Apache Spark 4.3](10_apache_spark_43/) | AWS-DEV · SQL, or local Apache Spark 4.3.0 | Open the [Spark 4.3 release notes](https://spark.apache.org/releases/spark-release-4-3-0.html) and run `orders.sql` on AWS. `JSON_TABLE` returns tea × 2 and coffee × 1. To show the actual upstream version, run `C:\git\temp\news-202621-spark43-venv\Scripts\python.exe .\demo.py` locally. The AWS SQL engine reports Spark 4.2.0. |
| 11 | [ASOF JOIN in Spark 4.3](11_asof_join/) | **AZURE-MVPLINK · classic** | Run `databricks bundle deploy -p AZURE-MVPLINK` from this folder. Attach `demo.sql` and `windows.sql` to **news-202621-asof**. Run the enable-setting cell in each notebook. Show scores NULL, 20, 70, then the five bounded-window cases. Terminate the single-node cluster after filming. Its idle timeout is 10 minutes. |
| 12 | [Date arithmetic in job parameters](12_job_date_arithmetic/) | AWS-DEV · Jobs | In Jobs search for **news-202621-job-dates**. Open the **compare_dates** task to show the two parameter expressions and its output. To resolve new dates on camera, run `databricks bundle run compare_dates -p AWS-DEV`. |
| 13 | [Serverless logs in Unity Catalog](13_serverless_logs/) | AWS-DEV · workspace settings | Open **Settings → Compute → Log delivery**. Show where an admin selects a restricted volume. For actual files, an admin must have saved that workspace-wide destination. Then deploy the included bundle, run `databricks bundle run emit_logs -p AWS-DEV`, allow a few minutes, and bind `run_log_path` in `read_logs.sql` to this run’s log directory. |
| 14 | [Materialized views and streaming tables from Python](14_python_mv_st/) | AWS-DEV · serverless notebook | Run `demo.py` from the interactive notebook, one definition at a time. Show the four-row source definition, then `@dp.materialized_view` for revenue by city and `@dp.table` for streamed orders. Display both results in the final cell. Expected revenue is 160 for Berlin and 140 for Prague. Allow time for the first refresh. |
| 15 | [df.cache() on serverless](15_serverless_cache/) | AWS-DEV · serverless notebook | Run `demo.py`. Materialize the cached counts, inspect the storage level and plan, and run the final `unpersist()` cell. The three groups have 334, 333 and 333 rows. |
| 16 | [DECFLOAT in SQL](16_decfloat/) | AWS-DEV · SQL | Run both cells in `demo.sql`. Contrast DOUBLE’s `0.30000000000000004` with decimal `0.3` (which can display as `+3E-1`), then compare the ranges of DECFLOAT(16) and (34). |
| 17 | [VARIANT transformations](17_variant_transformations/) | AWS-DEV · SQL | Run `demo.sql`. Start with the order status changing from new to paid. Continue through insert, delete, array append and null stripping. Finish with the `try_` functions on incompatible inputs. |
| 18 | [UNDROP VOLUME](18_undrop_volume/) | AWS-DEV · SQL via PowerShell | Run `.\run.ps1 -WarehouseId $warehouse`. Watch it create a disposable volume, find the dropped volume ID, restore it, display its name, and clean up its own schema. |
| 19 | [Unity Catalog wikis (Beta)](19_uc_wikis/) | AWS-DEV · Catalog Explorer | Open **sbx_test_data.news_202621.wiki_orders → Overview → Wiki**. Use the table comment to draft the grain, tax treatment and cancelled-order caveat. `orders.sql` reproduces the table with the `catalog` parameter set to `sbx_test_data`. The Wiki tab requires the account preview and suitable permissions to edit. |
| 20 | [JDBC connections in Unity Catalog](20_jdbc_connections/) | AWS-DEV · Catalog Explorer | Open **Catalog → Connect → Connections → Create connection → JDBC**. Show the driver JAR, URL and options allowlist. AWS-DEV has no generic JDBC connection. For a live query, supply a reachable database and driver in `create_connection.sql`, then run `demo.sql` with its connection name. |

## AI

| # | Article subtitle and folder | Workspace | What to show |
|---|---|---|---|
| 21 | [ChatGPT subscription with Codex through Unity Gateway](21_chatgpt_subscription/) | AWS-DEV · local terminal | Run `.\run.ps1` to inspect the existing OpenAI subscription provider **sbx_test_data.default.chatgpt**. Run `.\run.ps1 -Launch` to open Codex with it. The Gateway CLI is installed in the recording shell path above. Complete any interactive account prompt off camera. |
| 22 | [AI Functions Asynchronous API](22_ai_functions_async/) | AWS-DEV · serverless notebook + UC volume | In Catalog Explorer, open volume **sbx_test_data.news_202621.inputs**, then **support_ticket_batch**. Show the two requests and the completed JSONL in **output** (ticket-1 → billing, ticket-2 → technical). To submit again, run `batch.py` in cell order and watch the completion counts. |
| 23 | [Audio transcription in SQL](23_audio_transcription/) | AWS-DEV · serverless notebook | Open `transcribe.py`. The first cell reads the included eight-second WAV, the second shows SQL transcription, start time and speaker ID. The sample begins “The deployment is complete” and ends “Please prepare the sales dashboard.” |
| 24 | [Budget spend status API](24_budget_spend_status/) | **AWS account** · filtered to AWS-DEV | Run `.\spend_status.ps1 -Example` for the labelled offline response shape. For a live read, renew the billing-scoped **AWS-DEV-ACCOUNT** session first, then run `.\spend_status.ps1`. An account without a matching blocking budget can return an empty list. |
| 25 | [Budget alerts with webhooks](25_budget_webhooks/) | **AWS account console** + local replay | In **Budgets**, show **Trigger webhook** on a threshold if the account-level Webhook Destinations preview is enabled. Run `.\replay.ps1` to process the included synthetic event twice and show the duplicate skipped. Actual delivery requires a configured account notification destination. |
| 26 | [Agent memory](26_agent_memory/) | AWS-DEV · serverless notebook + Lakebase | In `agent_memory.py`, run the install and restart cells first. Save the recipe preference, then use a fresh client to recall it. Open the printed backing database in the Lakebase SQL editor and run `inspect_lakebase.sql`. Provisioning can take time. After filming select **yes** in `remove_after_filming` and rerun only the last cell to delete the demo store. |

For the live account-budget segment, renew the existing profile before recording. Run the following in PowerShell, open the displayed authorization URL in the in-app browser, then rerun chapter 24.

```powershell
pwsh -NoProfile -Command { $env:BROWSER = 'none'; & C:\git\cli\databricks.exe auth login --profile AWS-DEV-ACCOUNT --scopes 'billing,offline_access' }
```
