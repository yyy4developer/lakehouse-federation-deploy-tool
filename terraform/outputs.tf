# =============================================================================
# Outputs
# =============================================================================

# ----- AWS Glue -----
output "s3_bucket_name" {
  description = "S3 bucket for Glue data"
  value       = var.enable_glue ? aws_s3_bucket.glue_data[0].id : null
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = var.enable_glue ? aws_glue_catalog_database.factory_master[0].name : null
}

output "glue_iam_role_arn" {
  description = "IAM role ARN for Databricks Glue access"
  value       = var.enable_glue ? aws_iam_role.databricks_glue[0].arn : null
}

# ----- Redshift -----
output "redshift_endpoint" {
  description = "Redshift Serverless endpoint"
  value       = var.enable_redshift ? aws_redshiftserverless_workgroup.demo[0].endpoint[0].address : null
}

# ----- PostgreSQL -----
output "postgres_endpoint" {
  description = "PostgreSQL endpoint"
  value = var.enable_postgres ? (
    var.cloud == "aws" ? aws_db_instance.postgres[0].address : azurerm_postgresql_flexible_server.postgres[0].fqdn
  ) : null
}

# ----- Azure Synapse -----
output "synapse_endpoint" {
  description = "Azure Synapse SQL endpoint"
  value       = var.enable_synapse ? "${azurerm_synapse_workspace.demo[0].name}-ondemand.sql.azuresynapse.net" : null
}

# ----- BigQuery -----
output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = var.enable_bigquery ? google_bigquery_dataset.factory[0].dataset_id : null
}

# ----- Databricks Catalogs -----
output "databricks_catalogs" {
  description = "Map of deployed Databricks foreign catalogs"
  value = merge(
    var.enable_glue ? { glue = databricks_catalog.glue[0].name } : {},
    var.enable_redshift ? { redshift = databricks_catalog.redshift[0].name } : {},
    var.enable_postgres ? { postgres = databricks_catalog.postgres[0].name } : {},
    var.enable_synapse ? { synapse = databricks_catalog.synapse[0].name } : {},
    var.enable_bigquery && var.gcp_credentials_json != "" ? { bigquery = databricks_catalog.bigquery[0].name } : {},
    var.enable_onelake ? { onelake = databricks_catalog.onelake[0].name } : {},
  )
}
