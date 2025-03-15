-- Databricks notebook source
-- First, we need to create a connection to the service. This can also be done through the UI. In real life, we will use, for example, Jira, but here, we use just the request bin.
CREATE CONNECTION alert
  TYPE HTTP
  OPTIONS (
    host 'https://eoohomz6hb9petw.m.pipedream.net',
    port '443',
    base_path '/',
    bearer_token 'bin'
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![agents1.png](./images/agents1.png "agents1.png")

-- COMMAND ----------

-- Request to the external service
SELECT http_request(
    conn => 'alert',
    method => 'POST',
    path => '/',
     json => to_json(named_struct(
      'text', 'this is alert from agent'
    ))
  );

-- COMMAND ----------

-- a function that will be used by the agent. COMMENT - metadata here is critical so it will understand how to use it
CREATE OR REPLACE FUNCTION main.egg_shop.raise_alert(
    alert_text STRING COMMENT 'alert_text: The text message to be sent as an alert'
  )
  RETURNS STRING
  LANGUAGE SQL
  COMMENT 'Function to send an alert to an external service'
  RETURN http_request(
    conn => 'alert',
    method => 'POST',
    path => '/',
    json => to_json(named_struct('text', alert_text))
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![agents2.png](./images/agents2.png "agents2.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![agents3.png](./images/agents3.png "agents3.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
