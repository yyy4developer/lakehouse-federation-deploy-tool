# =============================================================================
# S3 Bucket for Glue Table Data (CSV)
# =============================================================================

resource "aws_s3_bucket" "glue_data" {
  bucket_prefix = "${var.project_prefix}-glue-data-"
  force_destroy = true

  tags = {
    Name = "${var.project_prefix}-glue-data"
  }
}

resource "aws_s3_bucket_public_access_block" "glue_data" {
  bucket = aws_s3_bucket.glue_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Upload CSV data files to S3
# -----------------------------------------------------------------------------

resource "aws_s3_object" "sensors_csv" {
  bucket = aws_s3_bucket.glue_data.id
  key    = "factory_master/sensors/sensors.csv"
  source = "${path.module}/data/sensors.csv"
  etag   = filemd5("${path.module}/data/sensors.csv")
}

resource "aws_s3_object" "machines_csv" {
  bucket = aws_s3_bucket.glue_data.id
  key    = "factory_master/machines/machines.csv"
  source = "${path.module}/data/machines.csv"
  etag   = filemd5("${path.module}/data/machines.csv")
}
