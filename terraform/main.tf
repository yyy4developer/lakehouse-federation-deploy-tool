terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.58"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
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
# Azure Provider
# -----------------------------------------------------------------------------
provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

# -----------------------------------------------------------------------------
# Google Cloud Provider
# -----------------------------------------------------------------------------
provider "google" {
  project     = var.gcp_project_id
  region      = var.gcp_region
  credentials = var.gcp_credentials_json != "" ? var.gcp_credentials_json : null
}

# -----------------------------------------------------------------------------
# Databricks Provider (workspace-level, OAuth U2M via CLI)
# Run: databricks auth login --host <workspace-url>
# -----------------------------------------------------------------------------
provider "databricks" {
  host = var.databricks_host
}
