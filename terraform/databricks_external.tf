# =============================================================================
# Databricks External Location
# Governs access to the S3 path where Glue table data is stored.
# Required for Glue HMS Federation - the foreign catalog needs
# authorized paths covered by external locations.
# =============================================================================

resource "databricks_external_location" "glue_data" {
  name            = "${var.project_prefix}-glue-data"
  url             = "s3://${aws_s3_bucket.glue_data.id}/factory_master"
  credential_name = databricks_storage_credential.glue_storage.name
  skip_validation = true # IAM role propagation may take a few seconds

  comment = "External location for Glue factory master data (Lakehouse Federation demo)"

  depends_on = [aws_iam_role.databricks_storage, aws_iam_role_policy.s3_read_access]
}
