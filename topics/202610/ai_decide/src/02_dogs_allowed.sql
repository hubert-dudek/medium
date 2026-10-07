-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 2. Can I bring my dog?
-- MAGIC Read the hotel policy. The answer is a probability from **0 to 1**.

-- COMMAND ----------

SELECT ai_decide(
  'Small dogs are welcome for EUR 15 per night.',
  '{"dogs": {
    "type": "noul",
    "instructions": "Can I stay here with a small dog?"
  }}'
):response.answers.dogs.probability::DECIMAL(3,2) AS dogs_allowed_probability;