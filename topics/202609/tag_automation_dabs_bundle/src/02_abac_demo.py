# Databricks notebook source
# MAGIC %md
# MAGIC # Optional ABAC demo driven by `ta_demo_pii`
# MAGIC
# MAGIC This notebook connects the Tag Automation result to Unity Catalog ABAC:
# MAGIC
# MAGIC 1. columns already carry `ta_demo_pii`,
# MAGIC 2. the automation rolls the same tag up to the table,
# MAGIC 3. the ABAC policy checks both the table tag and the column tag,
# MAGIC 4. `ABAC_POLICY_DEFINITIONS` exposes the resulting policy metadata.
# MAGIC
# MAGIC By default this notebook is read-only and only prints the policy SQL. Set `apply_policy=true` and provide a test user or group to create it.

# COMMAND ----------

from __future__ import annotations

import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Existing catalog")
dbutils.widgets.text("schema_name", "tags_experiment", "Experiment schema")
dbutils.widgets.dropdown("apply_policy", "false", ["true", "false"], "Create the policy")
dbutils.widgets.text("mask_principal", "", "Test user or group")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
apply_policy = dbutils.widgets.get("apply_policy").lower() == "true"
mask_principal = dbutils.widgets.get("mask_principal").strip()

if not catalog_name:
    raise ValueError("catalog_name cannot be empty")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", schema_name):
    raise ValueError("Invalid schema_name")


def qi(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def qname(*parts: str) -> str:
    return ".".join(qi(part) for part in parts)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


schema_qn = qname(catalog_name, schema_name)
mask_function_qn = qname(catalog_name, schema_name, "ta_demo_mask_string")
policy_name = "ta_demo_mask_pii"
principal_placeholder = sql_string(mask_principal) if mask_principal else "'<test-user-or-group>'"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Confirm the table-level and column-level PII tags

# COMMAND ----------

pii_table_tags = spark.sql(
    f"""
    SELECT table_name, tag_name, tag_value
    FROM {qname(catalog_name, 'information_schema', 'table_tags')}
    WHERE schema_name = {sql_string(schema_name)}
      AND tag_name = 'ta_demo_pii'
    ORDER BY table_name
    """
)
display(pii_table_tags)

pii_column_tags = spark.sql(
    f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {qname(catalog_name, 'information_schema', 'column_tags')}
    WHERE schema_name = {sql_string(schema_name)}
      AND tag_name = 'ta_demo_pii'
    ORDER BY table_name, column_name
    """
)
display(pii_column_tags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Policy SQL
# MAGIC
# MAGIC All tagged columns in this small dataset are strings, allowing one simple masking UDF.

# COMMAND ----------

policy_sql = f"""
CREATE OR REPLACE FUNCTION {mask_function_qn}(input_value STRING)
RETURNS STRING
RETURN CASE
  WHEN input_value IS NULL THEN NULL
  ELSE '***MASKED***'
END;

CREATE OR REPLACE POLICY {qi(policy_name)}
ON SCHEMA {schema_qn}
COMMENT 'Demo column mask activated by the table-level PII tag and applied to PII-tagged columns'
COLUMN MASK {mask_function_qn}
TO {principal_placeholder}
FOR TABLES
WHEN has_tag('ta_demo_pii')
MATCH COLUMNS has_tag('ta_demo_pii') AS pii_column
ON COLUMN pii_column;
""".strip()

print(policy_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optionally create the policy

# COMMAND ----------

if apply_policy:
    if not mask_principal:
        raise ValueError("mask_principal is required when apply_policy=true")

    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {mask_function_qn}(input_value STRING)
        RETURNS STRING
        RETURN CASE
          WHEN input_value IS NULL THEN NULL
          ELSE '***MASKED***'
        END
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE POLICY {qi(policy_name)}
        ON SCHEMA {schema_qn}
        COMMENT 'Demo column mask activated by the table-level PII tag and applied to PII-tagged columns'
        COLUMN MASK {mask_function_qn}
        TO {sql_string(mask_principal)}
        FOR TABLES
        WHEN has_tag('ta_demo_pii')
        MATCH COLUMNS has_tag('ta_demo_pii') AS pii_column
        ON COLUMN pii_column
        """
    )
    print(f"Created policy {policy_name} for principal: {mask_principal}")
else:
    print("Read-only mode: the policy was not created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Observe policies through Information Schema

# COMMAND ----------

try:
    policy_definitions = spark.sql(
        f"""
        SELECT
          policy_name,
          policy_type,
          catalog_name,
          schema_name,
          securable_name,
          on_securable_type,
          to_principals,
          except_principals,
          when_condition,
          match_columns,
          created_by
        FROM {qname(catalog_name, 'information_schema', 'abac_policy_definitions')}
        WHERE catalog_name = {sql_string(catalog_name)}
          AND schema_name = {sql_string(schema_name)}
        ORDER BY policy_type, policy_name
        """
    )
    display(policy_definitions)
except Exception as exc:
    print(
        "ABAC_POLICY_DEFINITIONS could not be read. It is a preview relation and visibility depends on "
        "READ METADATA, MANAGE, or ownership of the policy scope."
    )
    print(str(exc)[:1200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optional masking test
# MAGIC
# MAGIC Run this query as a user included in `mask_principal`. It should show `***MASKED***` for the three tagged string columns after the PII roll-up automation has assigned `ta_demo_pii` to the table.

# COMMAND ----------

sample_query = f"""
SELECT customer_id, full_name, email_address, phone_number, country_code
FROM {qname(catalog_name, schema_name, 'customers')}
ORDER BY customer_id
""".strip()

print(sample_query)

if apply_policy:
    display(spark.sql(sample_query))
