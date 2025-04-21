-- Databricks notebook source
CREATE TABLE main.samples.incoming_messages (
  message_id STRING,
  message_body STRING
);

INSERT INTO main.samples.incoming_messages (message_id, message_body) VALUES
('1', 'Congratulations! You have won a prize. Click here to claim.'),
('2', 'Meeting at 10 AM tomorrow.'),
('3', 'Limited time offer! Buy now and save 50%.'),
('4', 'Reminder: Project deadline is next Monday.'),
('5', 'You have been selected for a special discount. Act now!');

-- COMMAND ----------

SELECT
  message_id,
  parse_json(ai_query(
    endpoint       => "databricks-meta-llama-3-1-8b-instruct",
    request        => CONCAT(
      "Analyze the text, is it SPAM?: '", 
      REGEXP_REPLACE(message_body, "[^a-zA-Z0-9\\s:]", ""), 
      "'. Reply in valid JSON with fields: spam (bool) and confidence (float)."
    ),
    responseFormat => '{
      "type": "json_schema",
      "json_schema": {
        "name": "text_classifier",
        "schema": {
          "type": "object",
          "properties": {
            "spam": {"type": "boolean"},
            "confidence": {"type": "number"}
          }
        }
      }
    }',
    failOnError    => true
  )) AS classification_result
FROM main.samples.incoming_messages;
