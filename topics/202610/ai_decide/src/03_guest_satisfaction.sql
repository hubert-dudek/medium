-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 3. Was the guest happy?
-- MAGIC Score the review: **0 = unhappy, 1 = mixed, 2 = happy**. The score can be fractional.

-- COMMAND ----------

SELECT ai_decide(
  'Great location, but a noisy room and cold breakfast.',
  '{"satisfaction": {
    "type": "score",
    "instructions": "How satisfied is this hotel guest?",
    "criteria": ["Unhappy", "Mixed feelings", "Happy"]
  }}'
):response.answers.satisfaction.score::DECIMAL(3,2) AS satisfaction_0_to_2;