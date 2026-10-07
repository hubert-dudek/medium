-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 1. Sort a travel expense
-- MAGIC Is this transport, a hotel, or food?

-- COMMAND ----------

SELECT ai_decide(
  'Train from Prague to Vienna, EUR 18.',
  '{"category": {
    "type": "choice",
    "instructions": "Categorize this expense.",
    "criteria": {"transport": null, "hotel": null, "food": null}
  }}'
):response.answers.category.choice::STRING AS expense_category;