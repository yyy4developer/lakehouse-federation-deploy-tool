# =============================================================================
# Databricks Credentials for Lakehouse Federation
#
# IMPORTANT: Credentials are created BEFORE IAM roles.
# The role ARN is constructed as a string (not referencing aws_iam_role)
# to break the circular dependency. Databricks auto-generates the
# external_id, which is then used to create the IAM trust policy.
#
# Pattern from official Terraform guide:
# https://registry.terraform.io/providers/databricks/databricks/latest/docs/guides/unity-catalog
# =============================================================================

# -----------------------------------------------------------------------------
# Service Credential: Glue API access
# Used by the Glue connection to access AWS Glue catalog metadata
# -----------------------------------------------------------------------------

resource "databricks_credential" "glue_service" {
  name    = "${var.project_prefix}-glue-service-cred"
  purpose = "SERVICE"

  aws_iam_role {
    # Constructed ARN - IAM role is created AFTER this credential
    role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.glue_role_name}"
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
    # Constructed ARN - IAM role is created AFTER this credential
    role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.storage_role_name}"
  }

  comment = "Storage credential for Glue S3 data access (Lakehouse Federation demo)"
}
