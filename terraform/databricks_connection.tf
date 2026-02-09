# =============================================================================
# Databricks Connections for Lakehouse Federation
# =============================================================================

# -----------------------------------------------------------------------------
# Connection to AWS Glue (Hive Metastore Federation)
# Requires: Service Credential + External Location
# -----------------------------------------------------------------------------

resource "databricks_connection" "glue" {
  name            = "${var.project_prefix}-glue-conn"
  connection_type = "GLUE"

  options = {
    aws_region     = var.aws_region
    aws_account_id = data.aws_caller_identity.current.account_id
    credential     = databricks_credential.glue_service.name
  }

  comment = "Connection to AWS Glue catalog for Lakehouse Federation demo"
}

# -----------------------------------------------------------------------------
# Connection to Amazon Redshift Serverless
# Uses username/password authentication
# -----------------------------------------------------------------------------

resource "databricks_connection" "redshift" {
  name            = "${var.project_prefix}-redshift-conn"
  connection_type = "REDSHIFT"

  options = {
    host     = aws_redshiftserverless_workgroup.demo.endpoint[0].address
    port     = "5439"
    user     = "admin"
    password = var.redshift_admin_password
  }

  comment = "Connection to Redshift Serverless for Lakehouse Federation demo"
}
