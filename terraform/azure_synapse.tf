# =============================================================================
# Azure Synapse Analytics (Serverless SQL pool)
# =============================================================================

resource "azurerm_storage_account" "synapse" {
  count = var.enable_synapse ? 1 : 0

  name                     = replace("${var.project_prefix}synapse", "-", "")
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

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  count = var.enable_synapse ? 1 : 0

  name               = "synapse"
  storage_account_id = azurerm_storage_account.synapse[0].id
}

resource "azurerm_synapse_workspace" "demo" {
  count = var.enable_synapse ? 1 : 0

  name                                 = "${var.project_prefix}-synapse"
  resource_group_name                  = azurerm_resource_group.demo[0].name
  location                             = azurerm_resource_group.demo[0].location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse[0].id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.synapse_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

# Split into two broad ranges to avoid Azure policy blocking 0.0.0.0/0
resource "azurerm_synapse_firewall_rule" "allow_broad1" {
  count = var.enable_synapse ? 1 : 0

  name                 = "AllowBroad1"
  synapse_workspace_id = azurerm_synapse_workspace.demo[0].id
  start_ip_address     = "1.0.0.0"
  end_ip_address       = "126.255.255.255"
}

resource "azurerm_synapse_firewall_rule" "allow_broad2" {
  count = var.enable_synapse ? 1 : 0

  name                 = "AllowBroad2"
  synapse_workspace_id = azurerm_synapse_workspace.demo[0].id
  start_ip_address     = "128.0.0.0"
  end_ip_address       = "254.255.255.255"
}

# Initialize Synapse with tables and data via sqlcmd
resource "null_resource" "synapse_init" {
  count = var.enable_synapse ? 1 : 0

  triggers = {
    workspace_id = azurerm_synapse_workspace.demo[0].id
  }

  provisioner "local-exec" {
    command = <<-EOT
      SYNAPSE_ONDEMAND="${azurerm_synapse_workspace.demo[0].name}-ondemand.sql.azuresynapse.net"
      DB_NAME="${local.synapse_db_name}"

      echo "Waiting 60s for firewall rules to propagate..."
      sleep 60

      echo "Creating serverless database $DB_NAME..."
      for i in $(seq 1 12); do
        # Try to create the database
        sqlcmd -S "$SYNAPSE_ONDEMAND" -d master --authentication-method=ActiveDirectoryDefault \
          -Q "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$DB_NAME') CREATE DATABASE $DB_NAME COLLATE Latin1_General_100_BIN2_UTF8" 2>&1 || true

        # Check if the database actually exists
        DB_EXISTS=$(sqlcmd -S "$SYNAPSE_ONDEMAND" -d master --authentication-method=ActiveDirectoryDefault \
          -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '$DB_NAME'" -h -1 2>/dev/null | tr -d '[:space:]')
        if [ "$DB_EXISTS" = "1" ]; then
          echo "  Database $DB_NAME created/verified."
          break
        fi
        echo "  Attempt $i/12: database not ready yet (waiting 15s)..."
        sleep 15
      done

      if [ "$DB_EXISTS" != "1" ]; then
        echo "ERROR: Failed to create database $DB_NAME after 12 attempts"
        exit 1
      fi

      SCHEMA_NAME="${local.source_schema}"
      echo "Creating schema $SCHEMA_NAME..."
      for i in $(seq 1 5); do
        sqlcmd -S "$SYNAPSE_ONDEMAND" -d $DB_NAME --authentication-method=ActiveDirectoryDefault \
          -Q "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '$SCHEMA_NAME') EXEC('CREATE SCHEMA [$SCHEMA_NAME]')" 2>&1 && break
        echo "  Retry schema creation $i/5 (waiting 10s)..."
        sleep 10
      done

      echo "Creating views with sample data..."
      VIEW_OK=0
      for i in $(seq 1 10); do
        TMPF1=$(mktemp) && TMPF2=$(mktemp)
        sed "s/dbo\./$SCHEMA_NAME./g" ${path.module}/sql/synapse/create_shift_schedules.sql > "$TMPF1"
        sed "s/dbo\./$SCHEMA_NAME./g" ${path.module}/sql/synapse/create_energy_consumption.sql > "$TMPF2"
        if sqlcmd -S "$SYNAPSE_ONDEMAND" -d $DB_NAME --authentication-method=ActiveDirectoryDefault \
          -i "$TMPF1" && \
           sqlcmd -S "$SYNAPSE_ONDEMAND" -d $DB_NAME --authentication-method=ActiveDirectoryDefault \
          -i "$TMPF2"; then
          VIEW_OK=1
          rm -f "$TMPF1" "$TMPF2"
          break
        fi
        rm -f "$TMPF1" "$TMPF2"
        echo "  Retry view creation $i/10 (waiting 15s)..."
        sleep 15
      done

      if [ "$VIEW_OK" -eq 0 ]; then
        echo "ERROR: Failed to create views after retries"
        exit 1
      fi

      echo "Verifying views..."
      sqlcmd -S "$SYNAPSE_ONDEMAND" -d $DB_NAME --authentication-method=ActiveDirectoryDefault \
        -Q "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='VIEW'"

      echo "Synapse initialization complete."
    EOT
  }

  depends_on = [azurerm_synapse_firewall_rule.allow_broad1, azurerm_synapse_firewall_rule.allow_broad2]
}
