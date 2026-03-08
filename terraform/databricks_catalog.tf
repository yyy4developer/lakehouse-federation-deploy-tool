# =============================================================================
# Databricks Foreign Catalogs
# Mirror external data sources in Unity Catalog
# =============================================================================

# -----------------------------------------------------------------------------
# Catalog Federation: AWS Glue
# -----------------------------------------------------------------------------
resource "databricks_catalog" "glue" {
  count           = var.enable_glue ? 1 : 0
  name            = "${var.catalog_prefix_catalog}_glue"
  connection_name = databricks_connection.glue[0].name

  options = {
    authorized_paths = "s3://${aws_s3_bucket.glue_data[0].id}"
  }

  storage_root = "s3://${aws_s3_bucket.glue_data[0].id}/glue_factory_metadata"

  comment = "外部カタログ: AWS Glue 工場マスタデータ（sensors, machines, quality_inspections）"

  depends_on = [databricks_external_location.glue_data]
}

# -----------------------------------------------------------------------------
# Query Federation: Redshift
# -----------------------------------------------------------------------------
resource "databricks_catalog" "redshift" {
  count           = var.enable_redshift ? 1 : 0
  name            = "${var.catalog_prefix_query}_redshift"
  connection_name = databricks_connection.redshift[0].name

  options = {
    database = local.redshift_db_name
  }

  comment = "外部カタログ: Redshift 工場トランザクションデータ（sensor_readings, production_events, quality_inspections）"
}

# -----------------------------------------------------------------------------
# Query Federation: PostgreSQL
# -----------------------------------------------------------------------------
resource "databricks_catalog" "postgres" {
  count           = var.enable_postgres ? 1 : 0
  name            = "${var.catalog_prefix_query}_postgres"
  connection_name = databricks_connection.postgres[0].name

  options = {
    database = local.postgres_db_name
  }

  comment = "外部カタログ: PostgreSQL 保守・作業指示データ（maintenance_logs, work_orders）"
}

# -----------------------------------------------------------------------------
# Query Federation: Azure Synapse
# -----------------------------------------------------------------------------
resource "databricks_catalog" "synapse" {
  count           = var.enable_synapse ? 1 : 0
  name            = "${var.catalog_prefix_query}_synapse"
  connection_name = databricks_connection.synapse[0].name

  options = {
    database = local.synapse_db_name
  }

  comment = "外部カタログ: Azure Synapse シフト・電力データ（shift_schedules, energy_consumption）"
}

# -----------------------------------------------------------------------------
# Query Federation: BigQuery
# -----------------------------------------------------------------------------
resource "databricks_catalog" "bigquery" {
  count           = var.enable_bigquery && var.gcp_credentials_json != "" ? 1 : 0
  name            = "${var.catalog_prefix_query}_bigquery"
  connection_name = databricks_connection.bigquery[0].name

  options = {
    dataProjectId = var.gcp_project_id
  }

  comment = "外部カタログ: BigQuery 稼働停止・コストデータ（downtime_records, cost_allocation）"
}

# -----------------------------------------------------------------------------
# Union Catalog: Analysis results (machine_health_summary etc.)
# On Azure: requires explicit storage_root (no metastore-level storage root)
# -----------------------------------------------------------------------------
resource "databricks_catalog" "union" {
  name          = var.analysis_catalog
  comment       = "分析結果カタログ: クロスソース JOIN テーブルを格納（machine_health_summary 等）"
  force_destroy = true

  storage_root = var.cloud == "azure" ? (
    "abfss://${azurerm_storage_container.catalog[0].name}@${azurerm_storage_account.catalog[0].name}.dfs.core.windows.net/${var.analysis_catalog}"
  ) : null

  depends_on = [databricks_external_location.catalog]
}

# -----------------------------------------------------------------------------
# Catalog Federation: OneLake
# -----------------------------------------------------------------------------
resource "databricks_catalog" "onelake" {
  count           = var.enable_onelake ? 1 : 0
  name            = "${var.catalog_prefix_catalog}_onelake"
  connection_name = databricks_connection.onelake[0].name

  comment = "外部カタログ: OneLake 生産計画・在庫データ（production_plans, inventory_levels）"
}
