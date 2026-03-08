# =============================================================================
# Azure Database for PostgreSQL Flexible Server
# =============================================================================

# Separate resource group for PostgreSQL (eastus may be restricted)
resource "azurerm_resource_group" "postgres" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name     = "${var.azure_resource_group_name}-postgres"
  location = "westus2"

  tags = {
    Project   = "lakehouse-federation-demo"
    ManagedBy = "terraform"
    owner     = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_postgresql_flexible_server" "postgres" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name                = "${var.project_prefix}-postgres"
  resource_group_name = azurerm_resource_group.postgres[0].name
  location            = azurerm_resource_group.postgres[0].location

  version                      = "16"
  sku_name                     = "B_Standard_B1ms"
  storage_mb                   = 32768
  administrator_login          = "pgadmin"
  administrator_password       = var.postgres_admin_password
  zone                         = "1"
  public_network_access_enabled = true

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_postgresql_flexible_server_database" "factory" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name      = local.postgres_db_name
  server_id = azurerm_postgresql_flexible_server.postgres[0].id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# Split into two ranges to avoid Azure policy blocking 0.0.0.0/0
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_broad1" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name             = "AllowBroad1"
  server_id        = azurerm_postgresql_flexible_server.postgres[0].id
  start_ip_address = "1.0.0.0"
  end_ip_address   = "126.255.255.255"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_broad2" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name             = "AllowBroad2"
  server_id        = azurerm_postgresql_flexible_server.postgres[0].id
  start_ip_address = "128.0.0.0"
  end_ip_address   = "254.255.255.255"
}

resource "null_resource" "azure_postgres_init" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  triggers = {
    server_id = azurerm_postgresql_flexible_server.postgres[0].id
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PGPASSWORD='${var.postgres_admin_password}'
      PGHOST='${azurerm_postgresql_flexible_server.postgres[0].fqdn}'
      SCHEMA='${local.source_schema}'

      # Find psql (macOS homebrew path or standard)
      PSQL=$(command -v psql || echo "/opt/homebrew/opt/libpq/bin/psql")
      if [ ! -x "$PSQL" ]; then
        echo "ERROR: psql not found. Install via: brew install libpq" >&2
        exit 1
      fi

      echo "Creating schema $SCHEMA..."
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA"

      # Set search_path so all SQL files create objects in the custom schema
      export PGOPTIONS="-c search_path=$SCHEMA"

      echo "Creating tables..."
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_machines.sql
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_maintenance_logs.sql
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_work_orders.sql

      echo "Inserting data..."
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_machines.sql
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_maintenance_logs.sql
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_work_orders.sql

      echo "Adding comments..."
      "$PSQL" -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/comments.sql

      echo "Azure PostgreSQL initialization complete."
    EOT
  }

  depends_on = [
    azurerm_postgresql_flexible_server_database.factory,
    azurerm_postgresql_flexible_server_firewall_rule.allow_broad1,
    azurerm_postgresql_flexible_server_firewall_rule.allow_broad2,
  ]
}
