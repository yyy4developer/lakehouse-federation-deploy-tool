# =============================================================================
# General
# =============================================================================

variable "project_prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "lhf-demo"
}

# =============================================================================
# AWS
# =============================================================================

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_account_id" {
  description = "Your AWS account ID"
  type        = string
}

# =============================================================================
# Databricks
# =============================================================================

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://e2-demo-field-eng.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "databricks_unity_catalog_aws_account_id" {
  description = "AWS Account ID used by Databricks Unity Catalog (for IAM trust policy). See README for how to obtain."
  type        = string
}

variable "databricks_external_id" {
  description = "External ID for Databricks Unity Catalog IAM role assumption. See README for how to obtain."
  type        = string
}

# =============================================================================
# Redshift
# =============================================================================

variable "redshift_admin_password" {
  description = "Redshift Serverless admin password (min 8 chars, must include uppercase, lowercase, and number)"
  type        = string
  sensitive   = true
}
