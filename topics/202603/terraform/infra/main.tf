terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = "acbc7e82-ebf1-4daf-96ab-26c1a4cb38be"
}

# Use your existing resource group
data "azurerm_resource_group" "rg" {
  name = "brickster_westus2"
}

# Get your current Azure tenant and user/service principal info
data "azurerm_client_config" "current" {}

# Add a random suffix so the Key Vault name is unique
resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "azurerm_key_vault" "kv" {
  name                       = "kvbrick${random_string.suffix.result}"
  location                   = data.azurerm_resource_group.rg.location
  resource_group_name        = data.azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  

  # Keep access-policy mode for Databricks secret scope integration
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get",
      "List",
      "Set"
    ]
  }
}

resource "azurerm_role_assignment" "kv_contributor_for_creator" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

locals {
  key_vault_dns_name = "https://${azurerm_key_vault.kv.name}.vault.azure.net/"
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}

output "key_vault_dns_name" {
  value = local.key_vault_dns_name
}

output "key_vault_resource_id" {
  value = azurerm_key_vault.kv.id
}

# This is the easiest output for DABS variable-overrides.json
output "dabs_variables" {
  value = {
    key_vault_dns_name    = local.key_vault_dns_name
    key_vault_resource_id = azurerm_key_vault.kv.id
  }
}
