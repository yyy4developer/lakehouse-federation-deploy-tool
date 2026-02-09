# =============================================================================
# Databricks Foreign Catalogs
# Mirror external data sources in Unity Catalog
# =============================================================================

# -----------------------------------------------------------------------------
# Foreign Catalog: AWS Glue (factory master data)
# Mirrors the Glue database as a Unity Catalog catalog.
# Authorized paths restrict which S3 locations can be accessed.
# -----------------------------------------------------------------------------

resource "databricks_catalog" "glue" {
  name            = "glue_factory"
  connection_name = databricks_connection.glue.name

  options = {
    authorized_paths = "s3://${aws_s3_bucket.glue_data.id}/factory_master"
  }

  comment = "Foreign catalog: AWS Glue factory master data (sensors, machines)"

  depends_on = [databricks_external_location.glue_data]
}

# -----------------------------------------------------------------------------
# Foreign Catalog: Redshift Serverless (factory transaction data)
# Mirrors the Redshift database as a Unity Catalog catalog.
# -----------------------------------------------------------------------------

resource "databricks_catalog" "redshift" {
  name            = "redshift_factory"
  connection_name = databricks_connection.redshift.name

  options = {
    database = "factory_db"
  }

  comment = "Foreign catalog: Redshift factory transaction data (sensor_readings, production_events)"
}
