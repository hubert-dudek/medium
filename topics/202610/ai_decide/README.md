# ai_decide: support-ticket triage with DABs

A small serverless job demonstrates all three `ai_decide` question types on six synthetic support tickets. The notebook turns the returned decisions into a routing table and an attention queue using SQL.

| Question | Type | Output used by this demo |
| --- | --- | --- |
| Which team owns the ticket? | `choice` | `platform`, `billing`, `access`, or `other` |
| Does it need immediate escalation? | `noul` | Probability from 0 to 1 |
| How urgent is it? | `score` | Fractional score from 0 to 2 for our three-item rubric |

## Run

Prerequisites:

- A Databricks CLI profile, permission to deploy/run jobs, and serverless Jobs compute.
- `ai_decide` Beta enabled in your workspace and available in its region. Check the official [function reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide) and your cloud's regional support page.
- The notebook uses Standard environment version 6, matching the neighboring demo. For a classic-compute adaptation, the function requires DBR 15.4 LTS or later; 18.2 or later is recommended. Databricks SQL Classic is unsupported.

```bash
cd topics/202610/ai_decide
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run ai_decide_demo -t dev -p DEFAULT
```

Replace `DEFAULT` with your profile if needed. Open the run's `triage_tickets` notebook output to see the grids. You can also open the deployed notebook and run it interactively on serverless compute for recording.

The job has no schedule, no external dependencies, no custom model endpoint, and no catalog/schema/table creation. Data and results live in session-local views. Job execution incurs normal compute and AI function charges; deployment does not run inference.

## What to show

1. Six input rows, including an outage, billing requests, and an ambiguous request.
2. One `ai_decide(body, '<questions JSON>', map('version', '1.0'))` expression with three question definitions.
3. The compact result grid: `ticket_id`, `team`, `escalation_probability`, `urgency_score`, and `result_status`.
4. The SQL `CASE` expression and resulting `next_action` column.
5. The filtered attention queue and the bundle's one-task job configuration.

See [SCREENPLAY.md](SCREENPLAY.md) for the timed short-video script and recording directions.

## How the demo stays repeatable

The question JSON is a constant SQL string; the ticket body changes per row. The notebook captures the six returned envelopes as JSON once, then recreates a small local-data-backed view. Result displays and routing queries read that snapshot, so they do not intentionally invoke the model again. Re-running the inference cell does invoke it again; infrastructure retries may also occur.

The bounded `collect()` is only for six demo records. For larger data, persist the inference output to a Delta table and query that table instead.

The original envelope is in `ticket_decisions_raw.decision_json`. It contains `response`, `metadata`, and `error_message`. For example, the chosen team is extracted from `$.response.answers.team.choice`. The notebook flags function errors and missing/out-of-range fields, routes those rows to human review, and fails the job at the final check. Argument or workspace errors can fail the inference query earlier.

Routing uses unrounded values and this explicit demo policy:

| First matching rule | Action |
| --- | --- |
| Function error or invalid response | `HUMAN_REVIEW` |
| Escalation probability >= 0.80 | `ESCALATE` |
| Team is `other` or team confidence < 0.70 | `HUMAN_REVIEW` |
| Urgency score >= 1.50 | `PRIORITIZE` |
| Otherwise | `ROUTE_TO_TEAM` |

The notebook produces recommendations only; it does not send tickets or notifications. Thresholds are illustrative, and model outputs can vary. Exact labels, scores, and routing counts are not test expectations. Rehearse once and record the actual results.

## Validation and cleanup

Checked locally: merged YAML against the official Databricks CLI v1.19.0 bundle schema, Python compilation, all eight SQL statements with a Databricks-dialect parser, notebook path resolution, and the constant question JSON. No authenticated workspace deployment or execution was performed while preparing this demo.

Local syntax/schema checks do not establish that a workspace has access to the Beta. Complete `bundle validate`, `deploy`, and `run` in your workspace, then confirm the last cell reports six validated decisions.

To remove this demo's deployed resources later:

```bash
databricks bundle destroy -t dev -p DEFAULT
```

## Sources

- [ai_decide SQL reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide)
- [Launch announcement, September 30, 2026](https://www.databricks.com/blog/introducing-aidecide-make-fast-decisions-your-governed-data)
- [Serverless bundle examples](https://docs.databricks.com/aws/en/dev-tools/bundles/examples#job-that-uses-serverless-compute)
- [try_variant_get](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_variant_get)
