-- Databricks notebook source
SELECT ai_decide(
  'This release is terrible. The search box crashes, and the new pricing is lousy value.',
  '{"moderation": {
    "type": "choice",
    "instructions": "Approve discussion and criticism. Refuse threats, harassment and scams.",
    "criteria": {
      "approved": null,
      "refused": null
    }
  }}'
):response.answers.moderation.choice::STRING AS decision;

-- COMMAND ----------

SELECT ai_query(
  'system.ai.meta-llama-3-3-70b-instruct',
  'Approve discussion and criticism. Refuse threats, harassment and scams.
Return exactly approved or refused.

Post: This release is terrible. The search box crashes, and the new pricing is lousy value.',
  modelParameters => named_struct('max_tokens', 16, 'temperature', 0.0)
) AS decision;
