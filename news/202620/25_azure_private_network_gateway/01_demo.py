# Databricks notebook source
# MAGIC %md
# MAGIC # Azure private network gateway
# MAGIC
# MAGIC Workspace: [Azure mvplink](https://adb-7405607825310844.4.azuredatabricks.net/)
# MAGIC
# MAGIC Compute: none. Architecture/documentation walkthrough; no gateway or network resource is provisioned.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open the official **Private network gateway** page and frame its architecture diagram.
# MAGIC 2. Trace serverless traffic through the gateway into the delegated subnet, then to VNet or connected-network resources.
# MAGIC 3. Show **Traffic modes**: `SPECIFIC_DESTINATIONS` and `ALL_TRAFFIC`.
# MAGIC 4. Open **Configure a private network gateway** > **Before you begin**. Point out the account REST API setup; do not look for an unsupported creation UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC The gateway connects serverless compute to resources reachable through the customer's VNet. It reuses an NCC and complements Private Link.
# MAGIC
# MAGIC Do not say all serverless compute is deployed inside that VNet. Private-endpoint and service-endpoint routing can take precedence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC Beta requires account-admin access, a supported region and Azure plan, a dedicated delegated subnet and reachable DNS. Current documentation provides no gateway creation UI or Terraform support.
# MAGIC
# MAGIC Present this as an architecture walkthrough unless an existing gateway has independently been verified. Do not change networking or claim a connectivity test.
# MAGIC
# MAGIC [Overview](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/private-network-gateway) · [Configuration](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/private-network-gateway/configure-private-network-gateway)
