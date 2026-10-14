# Databricks notebook source
# MAGIC %md
# MAGIC # Govern agent skills in Unity Catalog
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none. Inspect catalog metadata and documentation; do not launch an agent or publish a skill.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open **Catalog** and an existing schema intended for skills, if available.
# MAGIC 2. Show the namespace and existing permissions without editing grants.
# MAGIC 3. Show an existing skill's name and description if visible; otherwise open **Skills support in Unity Gateway** in the documentation.
# MAGIC 4. Frame the **Discover and use** skill-registry flow and the **Govern skills** privilege table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC A skill is a governed `catalog.schema.skill` object using the SKILL.md format. Consumers can load it live through the registry or download a point-in-time copy.
# MAGIC
# MAGIC Current documentation names the client **Unity Gateway CLI** (`ug`). Do not present the article's older “ucode” name as the current command.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC Skills are Beta and require account preview access and Unity Catalog. Consumers need catalog/schema traversal and skill read access; authors also need a managed storage-backed schema and create/write privileges.
# MAGIC
# MAGIC If no skill or preview is available, present a documentation walkthrough. Do not install tools, create skills or change grants for this chapter.
# MAGIC
# MAGIC [Overview](https://docs.databricks.com/aws/en/agents/uc-skills/) · [Discover skills](https://docs.databricks.com/aws/en/agents/uc-skills/use-uc-skills) · [Govern skills](https://docs.databricks.com/aws/en/ai-gateway/govern-skills)
