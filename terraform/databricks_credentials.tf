# =============================================================================
# Databricks Credentials for Lakehouse Federation
#
# 1. Service Credential  -> wraps Glue API IAM role
# 2. Storage Credential  -> wraps S3 read IAM role
#
# NOTE: databricks_credential (unified) requires provider >= 1.58
#       If your provider version doesn't support it, use the
#       Databricks UI or REST API to create the service credential.
# =============================================================================

# -----------------------------------------------------------------------------
# Service Credential: Glue API access
# Used by the Glue connection to access AWS Glue catalog metadata
# -----------------------------------------------------------------------------

resource "databricks_credential" "glue_service" {
  name    = "${var.project_prefix}-glue-service-cred"
  purpose = "SERVICE"

  aws_iam_role {
    role_arn = aws_iam_role.databricks_glue.arn
  }

  comment = "Service credential for AWS Glue API access (Lakehouse Federation demo)"
}

# -----------------------------------------------------------------------------
# Storage Credential: S3 data access
# Used by the external location to read Glue table data from S3
# -----------------------------------------------------------------------------

resource "databricks_storage_credential" "glue_storage" {
  name = "${var.project_prefix}-glue-storage-cred"

  aws_iam_role {
    role_arn = aws_iam_role.databricks_storage.arn
  }

  comment = "Storage credential for Glue S3 data access (Lakehouse Federation demo)"
}
