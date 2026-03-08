# =============================================================================
# General
# =============================================================================

variable "project_prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "lhf-demo"
}

variable "cloud" {
  description = "Cloud platform for the Databricks workspace (aws or azure)"
  type        = string
  default     = "aws"

  validation {
    condition     = contains(["aws", "azure"], var.cloud)
    error_message = "cloud must be 'aws' or 'azure'"
  }
}

# =============================================================================
# Federation Source Toggles
# =============================================================================

variable "enable_glue" {
  description = "Enable AWS Glue catalog federation"
  type        = bool
  default     = false
}

variable "enable_redshift" {
  description = "Enable Amazon Redshift query federation"
  type        = bool
  default     = false
}

variable "enable_postgres" {
  description = "Enable PostgreSQL query federation (RDS on AWS, Flexible Server on Azure)"
  type        = bool
  default     = false
}

variable "enable_synapse" {
  description = "Enable Azure Synapse Analytics query federation"
  type        = bool
  default     = false
}

variable "enable_bigquery" {
  description = "Enable Google BigQuery query federation"
  type        = bool
  default     = false
}

variable "enable_onelake" {
  description = "Enable Microsoft OneLake (Fabric) catalog federation"
  type        = bool
  default     = false
}

# =============================================================================
# Catalog Naming
# =============================================================================

variable "catalog_prefix_query" {
  description = "Prefix for query federation catalogs (e.g. lhf_query_redshift)"
  type        = string
  default     = "lhf_query"
}

variable "catalog_prefix_catalog" {
  description = "Prefix for catalog federation catalogs (e.g. lhf_catalog_glue)"
  type        = string
  default     = "lhf_catalog"
}

variable "analysis_catalog" {
  description = "Catalog name for analysis results (cross-source JOIN tables)"
  type        = string
  default     = "lhf_union_dbx"
}

variable "db_prefix" {
  description = "Prefix for source database/schema names (e.g. lhf_uq6v_demo → lhf_uq6v_demo_factory)"
  type        = string
  default     = ""
}

# =============================================================================
# Databricks
# =============================================================================

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://fevm-xxx.cloud.databricks.com)"
  type        = string
  default     = "https://fe-sandbox-serverless-sandbox-tjmjb6-yao.cloud.databricks.com"
}

# =============================================================================
# AWS
# =============================================================================

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "redshift_admin_password" {
  description = "Redshift Serverless admin password (min 8 chars, uppercase + lowercase + number)"
  type        = string
  sensitive   = true
  default     = ""
}

# =============================================================================
# Azure
# =============================================================================

variable "azure_subscription_id" {
  description = "Azure subscription ID (az account show --query id -o tsv)"
  type        = string
  default     = ""
}

variable "azure_region" {
  description = "Azure region for resources"
  type        = string
  default     = "eastus"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = "lhf-demo-rg"
}

variable "synapse_admin_password" {
  description = "Azure Synapse SQL admin password"
  type        = string
  sensitive   = true
  default     = ""
}

# =============================================================================
# GCP
# =============================================================================

variable "gcp_project_id" {
  description = "GCP project ID for BigQuery"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcp_credentials_json" {
  description = "GCP service account key JSON (base64 or raw). Leave empty to use gcloud auth."
  type        = string
  sensitive   = true
  default     = ""
}

# =============================================================================
# PostgreSQL
# =============================================================================

variable "postgres_admin_password" {
  description = "PostgreSQL admin password (min 8 chars)"
  type        = string
  sensitive   = true
  default     = ""
}

# =============================================================================
# OneLake / Microsoft Fabric
# =============================================================================

variable "fabric_workspace_id" {
  description = "Microsoft Fabric workspace ID (GUID). Leave empty to create new."
  type        = string
  default     = ""
}

variable "fabric_lakehouse_id" {
  description = "Pre-existing Fabric Lakehouse ID (GUID). Set to skip lakehouse creation via REST API."
  type        = string
  default     = ""
}
