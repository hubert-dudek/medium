-- Databricks notebook source
SELECT
  tickets.id,
  tickets.body,
  labels.response:response[0].value::STRING AS label
FROM news_202621_ai_functions.tickets AS tickets
JOIN news_202621_ai_functions.ticket_labels AS labels USING (id)
ORDER BY tickets.id;
