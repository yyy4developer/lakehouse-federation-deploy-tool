# =============================================================================
# Amazon Redshift Serverless
# Namespace + Workgroup with minimal capacity (8 RPUs) for cost savings
# =============================================================================

resource "aws_redshiftserverless_namespace" "demo" {
  namespace_name      = "${var.project_prefix}-ns"
  db_name             = "factory_db"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password

  tags = {
    Name = "${var.project_prefix}-namespace"
  }
}

resource "aws_redshiftserverless_workgroup" "demo" {
  workgroup_name = "${var.project_prefix}-wg"
  namespace_name = aws_redshiftserverless_namespace.demo.namespace_name

  base_capacity       = 8   # Minimum RPUs for cost savings
  publicly_accessible = true # Required for Databricks federation via public endpoint

  subnet_ids         = aws_subnet.public[*].id
  security_group_ids = [aws_security_group.redshift.id]

  tags = {
    Name = "${var.project_prefix}-workgroup"
  }
}
