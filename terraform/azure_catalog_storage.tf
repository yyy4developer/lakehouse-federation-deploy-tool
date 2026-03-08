# =============================================================================
# Azure storage for Databricks managed catalog (union catalog)
# Required on workspaces with Default Storage enabled (no metastore storage root)
# =============================================================================

resource "azurerm_storage_account" "catalog" {
  count = var.cloud == "azure" ? 1 : 0

  name                     = replace("${var.project_prefix}catalog", "-", "")
  resource_group_name      = azurerm_resource_group.demo[0].name
  location                 = azurerm_resource_group.demo[0].location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # ADLS Gen2

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_storage_container" "catalog" {
  count = var.cloud == "azure" ? 1 : 0

  name                  = "unity-catalog"
  storage_account_id    = azurerm_storage_account.catalog[0].id
  container_access_type = "private"
}

resource "azurerm_databricks_access_connector" "catalog" {
  count = var.cloud == "azure" ? 1 : 0

  name                = "${var.project_prefix}-access-connector"
  resource_group_name = azurerm_resource_group.demo[0].name
  location            = azurerm_resource_group.demo[0].location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_role_assignment" "catalog_storage" {
  count = var.cloud == "azure" ? 1 : 0

  scope                = azurerm_storage_account.catalog[0].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.catalog[0].identity[0].principal_id
}

resource "databricks_storage_credential" "catalog" {
  count = var.cloud == "azure" ? 1 : 0
  name  = "${var.project_prefix}-catalog-storage-cred"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.catalog[0].id
  }

  comment = "Storage credential for managed catalog storage"

  depends_on = [azurerm_role_assignment.catalog_storage]
}

resource "databricks_external_location" "catalog" {
  count = var.cloud == "azure" ? 1 : 0
  name  = "${var.project_prefix}-catalog-location"
  url   = "abfss://${azurerm_storage_container.catalog[0].name}@${azurerm_storage_account.catalog[0].name}.dfs.core.windows.net/"

  credential_name = databricks_storage_credential.catalog[0].name
  comment         = "External location for managed catalog storage"

  depends_on = [databricks_storage_credential.catalog]
}
