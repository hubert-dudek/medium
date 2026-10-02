# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ai_decide: six tickets, three decisions
# MAGIC Choose a team, estimate escalation probability, and score urgency in one SQL call per ticket.
# MAGIC Requires the **ai_decide Beta** in a supported region and serverless notebook/Jobs compute.
# MAGIC All input is synthetic. Only session-local views are created; no catalog or schema setup is needed.

# COMMAND ----------

# 1. Six short tickets for an easy-to-read recording.
tickets = [
    (1, "Production stopped", "All production pipelines have failed for two hours. No data reaches any dashboard and there is no workaround."),
    (2, "Duplicate charge", "Our subscription was charged twice this month. Please refund the extra charge. The service works normally."),
    (3, "New analyst access", "Please add our new analyst to the reporting group before they join next month. Nothing is blocked today."),
    (4, "Slow daily load", "The daily load is slow. A manual retry works, but we need the reports ready for a meeting this afternoon."),
    (5, "Invoice details", "Please change the company address on our next invoice. There is no deadline and the current invoice is paid."),
    (6, "Need help", "Something seems wrong. Can someone help? I have no more details yet."),
]
spark.createDataFrame(tickets, "ticket_id INT, subject STRING, body STRING").createOrReplaceTempView("tickets")
display(spark.sql("SELECT ticket_id, subject, body FROM tickets ORDER BY ticket_id"))

# COMMAND ----------

# 2. The JSON questions are a SQL string literal, shared by every row.
# Collect only these SIX demo rows. Keep a snapshot so later displays do not
# re-evaluate ai_decide. For real datasets, materialize to a Delta table instead.
decision_rows = spark.sql("""
SELECT
  ticket_id,
  subject,
  to_json(ai_decide(
    body,
    '{
      "team": {
        "type": "choice",
        "instructions": "Which team should own this ticket? Use other when there is not enough detail.",
        "criteria": {
          "platform": "Pipelines, service failures, and performance problems",
          "billing": "Charges, refunds, invoices, and subscription payments",
          "access": "User accounts, group membership, and permissions",
          "other": "Unclear requests or issues outside the listed teams"
        }
      },
      "needs_escalation": {
        "type": "noul",
        "instructions": "Does this ticket require an immediate incident escalation?",
        "criteria": {
          "true": "Production work is currently blocked and no workaround is available",
          "false": "Work can continue, a workaround exists, or no active outage is described"
        }
      },
      "urgency": {
        "type": "score",
        "instructions": "How urgent is this request based on the described business impact?",
        "criteria": [
          "Routine request without immediate business impact",
          "Time-sensitive issue with a usable workaround",
          "Critical production interruption without a workaround"
        ]
      }
    }',
    map('version', '1.0')
  )) AS decision_json
FROM tickets
""").collect()

spark.createDataFrame(
    decision_rows, "ticket_id INT, subject STRING, decision_json STRING"
).createOrReplaceTempView("ticket_decisions_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the VARIANT response
# MAGIC `choice` selects a label. `noul` returns a probability, not a Boolean.
# MAGIC With three ordered criteria, `score` is a weighted average from **0 to 2**, including fractions.
# MAGIC `team_confidence` describes support for the assessment; it is separate from each label's probability.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ticket_decisions AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT ticket_id, subject, parse_json(decision_json) AS decision
# MAGIC   FROM ticket_decisions_raw
# MAGIC ), typed AS (
# MAGIC   SELECT
# MAGIC     ticket_id,
# MAGIC     subject,
# MAGIC     try_variant_get(decision, '$.response.answers.team.choice', 'STRING') AS team,
# MAGIC     try_variant_get(decision, '$.response.answers.team.confidence', 'DOUBLE') AS team_confidence,
# MAGIC     try_variant_get(decision, '$.response.answers.needs_escalation.probability', 'DOUBLE') AS escalation_probability,
# MAGIC     try_variant_get(decision, '$.response.answers.urgency.score', 'DOUBLE') AS urgency_score,
# MAGIC     try_variant_get(decision, '$.error_message', 'STRING') AS error_message
# MAGIC   FROM parsed
# MAGIC )
# MAGIC SELECT *,
# MAGIC   CASE
# MAGIC     WHEN error_message IS NOT NULL THEN 'FUNCTION_ERROR'
# MAGIC     WHEN team IS NULL OR team NOT IN ('platform', 'billing', 'access', 'other')
# MAGIC       OR team_confidence IS NULL OR team_confidence NOT BETWEEN 0 AND 1
# MAGIC       OR escalation_probability IS NULL OR escalation_probability NOT BETWEEN 0 AND 1
# MAGIC       OR urgency_score IS NULL OR urgency_score NOT BETWEEN 0 AND 2
# MAGIC       THEN 'INVALID_RESPONSE'
# MAGIC     ELSE 'OK'
# MAGIC   END AS result_status
# MAGIC FROM typed;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. The main result grid for the video. Values can vary between runs.
# MAGIC SELECT ticket_id, team,
# MAGIC   round(escalation_probability, 2) AS escalation_probability,
# MAGIC   round(urgency_score, 2) AS urgency_score,
# MAGIC   result_status
# MAGIC FROM ticket_decisions
# MAGIC ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Ordinary SQL chooses the next action using the unrounded values.
# MAGIC -- These thresholds are demo policy choices, not Databricks defaults.
# MAGIC CREATE OR REPLACE TEMP VIEW ticket_actions AS
# MAGIC SELECT *,
# MAGIC   CASE
# MAGIC     WHEN result_status <> 'OK' THEN 'HUMAN_REVIEW'
# MAGIC     WHEN escalation_probability >= 0.80 THEN 'ESCALATE'
# MAGIC     WHEN team = 'other' OR team_confidence < 0.70 THEN 'HUMAN_REVIEW'
# MAGIC     WHEN urgency_score >= 1.50 THEN 'PRIORITIZE'
# MAGIC     ELSE 'ROUTE_TO_TEAM'
# MAGIC   END AS next_action
# MAGIC FROM ticket_decisions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticket_id, subject, team, next_action
# MAGIC FROM ticket_actions
# MAGIC ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filter the already-evaluated results into an attention queue.
# MAGIC SELECT ticket_id, team, next_action
# MAGIC FROM ticket_actions
# MAGIC WHERE next_action <> 'ROUTE_TO_TEAM'
# MAGIC ORDER BY ticket_id;

# COMMAND ----------

# 5. A completed SQL query can still contain per-row AI errors.
# Show errors and fail the job rather than reporting a successful demo run.
# Do not assert exact model-generated teams or scores: answers can vary.
invalid_rows = spark.sql("""
SELECT ticket_id, result_status, error_message
FROM ticket_decisions
WHERE result_status <> 'OK'
""").collect()
if invalid_rows:
    raise RuntimeError(f"ai_decide returned unsuccessful decisions: {invalid_rows}")

print(f"Validated {len(decision_rows)} decisions. Review the result grids above.")

