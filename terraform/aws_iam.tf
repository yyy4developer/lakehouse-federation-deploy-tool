# =============================================================================
# IAM Roles for Databricks Lakehouse Federation
#
# Pattern: Create Databricks credentials first (with constructed ARN),
# then use the auto-generated external_id to build IAM trust policies.
# =============================================================================

data "aws_caller_identity" "current" {}

locals {
  aws_account_id = data.aws_caller_identity.current.account_id

  # Pre-compute role names (used to construct ARNs before roles exist)
  glue_role_name    = "${var.project_prefix}-databricks-glue"
  storage_role_name = "${var.project_prefix}-databricks-storage"

  # Glue database name: uses db_prefix for consistency with other sources
  glue_database_name = "${local.db_prefix}_factory_master"

  # Networking needed?
  needs_aws_networking = var.enable_redshift || (var.enable_postgres && var.cloud == "aws")
}

# =============================================================================
# Role 1: Glue API Access (for Databricks Service Credential)
# =============================================================================

data "databricks_aws_unity_catalog_assume_role_policy" "glue" {
  count = var.enable_glue ? 1 : 0

  aws_account_id = local.aws_account_id
  role_name      = local.glue_role_name
  external_id    = databricks_credential.glue_service[0].aws_iam_role[0].external_id
}

resource "aws_iam_role" "databricks_glue" {
  count = var.enable_glue ? 1 : 0

  name               = local.glue_role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.glue[0].json
}

resource "aws_iam_role_policy" "glue_access" {
  count = var.enable_glue ? 1 : 0

  name = "${var.project_prefix}-glue-access"
  role = aws_iam_role.databricks_glue[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueReadAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:GetUserDefinedFunction",
          "glue:GetUserDefinedFunctions",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:catalog*",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:database/*",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:table/*/*",
        ]
      },
      {
        Sid      = "SelfAssumeRole"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = ["arn:aws:iam::${local.aws_account_id}:role/${local.glue_role_name}"]
      }
    ]
  })
}

# =============================================================================
# Role 2: S3 Data Access (for Databricks Storage Credential)
# =============================================================================

data "databricks_aws_unity_catalog_assume_role_policy" "storage" {
  count = var.enable_glue ? 1 : 0

  aws_account_id = local.aws_account_id
  role_name      = local.storage_role_name
  external_id    = databricks_storage_credential.glue_storage[0].aws_iam_role[0].external_id
}

resource "aws_iam_role" "databricks_storage" {
  count = var.enable_glue ? 1 : 0

  name               = local.storage_role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.storage[0].json
}

resource "aws_iam_role_policy" "s3_read_access" {
  count = var.enable_glue ? 1 : 0

  name = "${var.project_prefix}-s3-access"
  role = aws_iam_role.databricks_storage[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.glue_data[0].arn,
          "${aws_s3_bucket.glue_data[0].arn}/*",
        ]
      },
      {
        Sid    = "S3WriteAccessForCatalogMetadata"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          "${aws_s3_bucket.glue_data[0].arn}/glue_factory_metadata/*",
        ]
      },
      {
        Sid      = "SelfAssumeRole"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = ["arn:aws:iam::${local.aws_account_id}:role/${local.storage_role_name}"]
      }
    ]
  })
}
