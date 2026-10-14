# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Key Vault-backed Unity Catalog secrets
# MAGIC
# MAGIC Workspace: [Azure mvplink](https://adb-7405607825310844.4.azuredatabricks.net/)
# MAGIC
# MAGIC Compute: none. Show metadata/configuration only; never open or retrieve secret values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. In **Catalog**, open an existing externally backed schema, if available.
# MAGIC 2. Show its external secrets manager setting and connection type.
# MAGIC 3. If no setup exists, open **+** > **Create a connection**, select **Azure Key Vault**, frame the service-credential and vault-name fields, then cancel.
# MAGIC 4. Use the documentation to show the schema's external-secrets **Enable** setting. Do not enable it or create credentials.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC Values stay in Azure Key Vault. Unity Catalog exposes governed `catalog.schema.secret` objects; secret creation and rotation remain in Key Vault.
# MAGIC
# MAGIC This differs from workspace secret scopes. Explain reference/read privileges without displaying values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC External secrets are Beta. Setup needs the workspace preview, Unity Catalog, an authorized service credential and an Azure Key Vault connection. The schema must not contain Databricks-managed secrets when switching backends.
# MAGIC
# MAGIC If prerequisites are absent, record the documented flow only. Do not delete secrets or modify Azure roles/networking.
# MAGIC
# MAGIC [Configuration](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/configure-external-secrets) · [External-secret behavior](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/external-secrets)
