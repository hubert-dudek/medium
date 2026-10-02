-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ai_decide: three everyday questions
-- MAGIC Each example is one independent SQL query. Run any cell on its own.

-- COMMAND ----------

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

-- COMMAND ----------

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

-- COMMAND ----------

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

