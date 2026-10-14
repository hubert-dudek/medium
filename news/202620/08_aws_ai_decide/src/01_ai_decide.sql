-- Databricks notebook source
SELECT ai_decide(
  'Train from Prague to Vienna, EUR 18.',
  '{"category": {
    "type": "choice",
    "instructions": "Categorize this expense.",
    "criteria": {"transport": null, "hotel": null, "food": null}
  }}'
):response.answers.category.choice::STRING AS expense_category;

-- COMMAND ----------

SELECT ai_decide(
  'Small dogs are welcome for EUR 15 per night.',
  '{"dogs": {
    "type": "noul",
    "instructions": "Can I stay here with a small dog?"
  }}'
):response.answers.dogs.probability::DECIMAL(3,2) AS dogs_allowed_probability;

-- COMMAND ----------

SELECT ai_decide(
  'Great location, but a noisy room and cold breakfast.',
  '{"satisfaction": {
    "type": "score",
    "instructions": "How satisfied is this hotel guest?",
    "criteria": ["Unhappy", "Mixed feelings", "Happy"]
  }}'
):response.answers.satisfaction.score::DECIMAL(3,2) AS satisfaction_0_to_2;
