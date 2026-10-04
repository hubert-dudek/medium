# Recording walkthrough — simple UI demo

Suggested title: **Databricks Serverless Environment Variables: UI, YAML and Python**

The job is deployed and configured. Open its task settings, `config/application.env`, and the notebooks in `src` before recording. No helper script is needed.

| Time | Show | Say |
|---|---|---|
| 0:00–0:25 | `01_simple.py` and output | “Environment variables are named values outside our code. Python reads them with os.environ or os.getenv.” |
| 0:25–1:00 | Environment variables editor | “app_config names the whole set. Variables are the inline values. Files are paths to text files with more values. A task selects one set.” |
| 1:00–1:25 | `application.env`, then output | “The file says WARNING. The inline value says DEBUG, so the task gets DEBUG. FILE_ONLY comes from the file.” |
| 1:25–1:55 | `examples/job-environment.yml` | “This is the job configuration shape. The key connects a task to app_config. In CLI 1.19.0 these native bundle fields are still dropped, so this demo uses the UI to configure them.” |
| 1:55–2:30 | `app_settings.py` and `02_import_settings.py` | “app_settings is just our own Python module. It stores configuration in one place. Importing it runs those assignments once in this Python process.” |
| 2:30–3:00 | `03_typed_config.py` | “Environment values are strings. Convert numbers and Booleans before using them. Do not use bool on the string false: it is a non-empty string.” |
| 3:00–3:30 | `04_udf_boundary.py` output | “The marker is available in the task process. The Spark UDF returns null: this setting does not configure Spark workers.” |
| 3:30–3:50 | `05_without_entry.py` | “A task without a selected entry does not get these demo variables.” |
| 3:50–4:00 | Return to the first notebook | “Set configuration in the job, keep Python simple, and check the task and Spark boundaries.” |

For an optional second recording, change APP_ENV from staging to production in the UI and run the job again. The same Python code will read the changed value. Record a new job run instead of relying on a cached import in an already-running interactive notebook.
