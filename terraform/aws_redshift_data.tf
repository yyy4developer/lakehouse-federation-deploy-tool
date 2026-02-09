# =============================================================================
# Redshift Data API Statements
# Creates tables and inserts dummy data into Redshift Serverless
# =============================================================================

# -----------------------------------------------------------------------------
# DDL: Create Tables
# -----------------------------------------------------------------------------

resource "aws_redshiftdata_statement" "create_sensor_readings" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/create_sensor_readings.sql")
}

resource "aws_redshiftdata_statement" "create_production_events" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/create_production_events.sql")
}

# -----------------------------------------------------------------------------
# DML: Insert Dummy Data
# -----------------------------------------------------------------------------

resource "aws_redshiftdata_statement" "insert_sensor_readings" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/insert_sensor_readings.sql")

  depends_on = [aws_redshiftdata_statement.create_sensor_readings]
}

resource "aws_redshiftdata_statement" "insert_production_events" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/insert_production_events.sql")

  depends_on = [aws_redshiftdata_statement.create_production_events]
}
