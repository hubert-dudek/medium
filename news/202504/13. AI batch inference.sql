-- Databricks notebook source
SELECT
  message_id,
  parse_json(ai_query(
    endpoint       => "databricks-meta-llama-3-1-8b-instruct",
    request        => CONCAT(
      "Analyze the text: '", 
      REGEXP_REPLACE(message_body, "[^a-zA-Z0-9\\s:]", ""), 
      "'. Reply in valid JSON with fields: label (string) and confidence (float)."
    ),
    responseFormat => '{
      "type": "json_schema",
      "json_schema": {
        "name": "text_classifier",
        "schema": {
          "type": "object",
          "properties": {
            "label": {"type": "string"},
            "confidence": {"type": "number"}
          }
        }
      }
    }',
    failOnError    => true
  )) AS classification_result
FROM incoming_messages;
