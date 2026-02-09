# =============================================================================
# Outputs
# =============================================================================

# ----- AWS -----

output "s3_bucket_name" {
  description = "S3 bucket containing Glue table data"
  value       = aws_s3_bucket.glue_data.id
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.factory_master.name
}

output "redshift_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = aws_redshiftserverless_workgroup.demo.endpoint[0].address
}

output "redshift_database" {
  description = "Redshift database name"
  value       = "factory_db"
}

output "glue_iam_role_arn" {
  description = "IAM role ARN for Databricks Glue access (service credential)"
  value       = aws_iam_role.databricks_glue.arn
}

output "storage_iam_role_arn" {
  description = "IAM role ARN for Databricks S3 access (storage credential)"
  value       = aws_iam_role.databricks_storage.arn
}

# ----- Databricks -----

output "databricks_glue_catalog" {
  description = "Databricks foreign catalog name for Glue"
  value       = databricks_catalog.glue.name
}

output "databricks_redshift_catalog" {
  description = "Databricks foreign catalog name for Redshift"
  value       = databricks_catalog.redshift.name
}

output "databricks_glue_connection" {
  description = "Databricks connection name for Glue"
  value       = databricks_connection.glue.name
}

output "databricks_redshift_connection" {
  description = "Databricks connection name for Redshift"
  value       = databricks_connection.redshift.name
}
