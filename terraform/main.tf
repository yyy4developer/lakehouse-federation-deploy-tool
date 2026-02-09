terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.58"
    }
  }
}

# -----------------------------------------------------------------------------
# AWS Provider
# -----------------------------------------------------------------------------
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "lakehouse-federation-demo"
      ManagedBy = "terraform"
    }
  }
}

# -----------------------------------------------------------------------------
# Databricks Provider (workspace-level)
# -----------------------------------------------------------------------------
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
