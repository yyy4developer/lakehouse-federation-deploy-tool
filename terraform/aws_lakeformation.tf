# =============================================================================
# AWS Lake Formation Permissions
# Required when Lake Formation is enabled on the AWS account.
#
# The account's CreateTableDefaultPermissions already grants
# IAM_ALLOWED_PRINCIPALS on new tables. However, CreateDatabaseDefaultPermissions
# is empty, so we must explicitly grant IAM_ALLOWED_PRINCIPALS on our database.
# =============================================================================

# Grant IAM_ALLOWED_PRINCIPALS access on the Glue database
# so that IAM policies alone can control access (opt-out of LF for this DB)
resource "aws_lakeformation_permissions" "iam_database" {
  principal   = "IAM_ALLOWED_PRINCIPALS"
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.factory_master.name
  }
}
