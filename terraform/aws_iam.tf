# =============================================================================
# IAM Roles for Databricks Lakehouse Federation
#
# Two roles are needed:
#   1. Glue API access  -> Databricks Service Credential
#   2. S3 data access   -> Databricks Storage Credential
#
# Both roles must:
#   - Trust Databricks Unity Catalog's AWS account (cross-account assume)
#   - Be self-assuming (required by Databricks service/storage credentials)
# =============================================================================

locals {
  # Pre-compute role names and ARNs to avoid circular references in trust policies
  glue_role_name    = "${var.project_prefix}-databricks-glue"
  storage_role_name = "${var.project_prefix}-databricks-storage"
  glue_role_arn     = "arn:aws:iam::${var.aws_account_id}:role/${local.glue_role_name}"
  storage_role_arn  = "arn:aws:iam::${var.aws_account_id}:role/${local.storage_role_name}"

  # Glue database name (underscores for SQL compatibility)
  glue_database_name = replace("${var.project_prefix}_factory_master", "-", "_")
}

# =============================================================================
# Role 1: Glue API Access (for Databricks Service Credential)
# =============================================================================

resource "aws_iam_role" "databricks_glue" {
  name = local.glue_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksUCAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_unity_catalog_aws_account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_external_id
          }
        }
      },
      {
        Sid    = "SelfAssume"
        Effect = "Allow"
        Principal = {
          AWS = local.glue_role_arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_access" {
  name = "${var.project_prefix}-glue-access"
  role = aws_iam_role.databricks_glue.id

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
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/${local.glue_database_name}",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${local.glue_database_name}/*",
        ]
      }
    ]
  })
}

# =============================================================================
# Role 2: S3 Data Access (for Databricks Storage Credential)
# =============================================================================

resource "aws_iam_role" "databricks_storage" {
  name = local.storage_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksUCAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_unity_catalog_aws_account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_external_id
          }
        }
      },
      {
        Sid    = "SelfAssume"
        Effect = "Allow"
        Principal = {
          AWS = local.storage_role_arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "s3_read_access" {
  name = "${var.project_prefix}-s3-read"
  role = aws_iam_role.databricks_storage.id

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
          aws_s3_bucket.glue_data.arn,
          "${aws_s3_bucket.glue_data.arn}/*",
        ]
      },
      {
        Sid    = "STSAccess"
        Effect = "Allow"
        Action = [
          "sts:GetCallerIdentity",
        ]
        Resource = "*"
      }
    ]
  })
}
