# =============================================================================
# Glue ETL Job for Data Generation
# =============================================================================

resource "aws_iam_role" "glue_etl" {
  count = var.enable_glue ? 1 : 0
  name  = "${var.project_prefix}-glue-etl"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_etl" {
  count = var.enable_glue ? 1 : 0
  name  = "${var.project_prefix}-glue-etl-policy"
  role  = aws_iam_role.glue_etl[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.glue_data[0].arn, "${aws_s3_bucket.glue_data[0].arn}/*"]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
          "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition", "glue:DeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:database/*",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:table/*/*",
        ]
      },
      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.aws_region}:${local.aws_account_id}:*"
      },
      {
        Sid      = "LakeFormationAccess"
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_etl_service" {
  count      = var.enable_glue ? 1 : 0
  role       = aws_iam_role.glue_etl[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_s3_object" "glue_script" {
  count  = var.enable_glue ? 1 : 0
  bucket = aws_s3_bucket.glue_data[0].id
  key    = "scripts/generate_data.py"
  source = "${path.module}/scripts/generate_data.py"
  etag   = filemd5("${path.module}/scripts/generate_data.py")
}

resource "aws_glue_job" "data_generator" {
  count    = var.enable_glue ? 1 : 0
  name     = "${var.project_prefix}-data-generator"
  role_arn = aws_iam_role.glue_etl[0].arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_data[0].id}/scripts/generate_data.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                = "delta,iceberg"
    "--enable-glue-datacatalog"         = ""
    "--S3_BUCKET"                       = aws_s3_bucket.glue_data[0].id
    "--GLUE_DATABASE"                   = aws_glue_catalog_database.factory_master[0].name
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${aws_s3_bucket.glue_data[0].id}/tmp/"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10

  depends_on = [
    aws_s3_object.glue_script,
    aws_glue_catalog_database.factory_master,
    aws_lakeformation_permissions.iam_database,
  ]
}

resource "null_resource" "run_glue_job" {
  count = var.enable_glue ? 1 : 0

  triggers = {
    script_hash = filemd5("${path.module}/scripts/generate_data.py")
    job_name    = aws_glue_job.data_generator[0].name
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting 15s for IAM role propagation..."
      sleep 15

      JOB_NAME="${aws_glue_job.data_generator[0].name}"
      REGION="${var.aws_region}"

      # Retry Glue job start up to 3 times (IAM propagation may need more time)
      for attempt in 1 2 3; do
        echo "Starting Glue job: $JOB_NAME (attempt $attempt)"
        RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --region "$REGION" --query 'JobRunId' --output text)
        echo "Job run ID: $RUN_ID"

        while true; do
          STATUS=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" --region "$REGION" --query 'JobRun.JobRunState' --output text)
          echo "Status: $STATUS"
          if [ "$STATUS" = "SUCCEEDED" ]; then
            echo "Glue job completed successfully."
            exit 0
          elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "STOPPED" ] || [ "$STATUS" = "ERROR" ] || [ "$STATUS" = "TIMEOUT" ]; then
            ERR=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" --region "$REGION" --query 'JobRun.ErrorMessage' --output text)
            echo "Glue job failed: $ERR"
            if echo "$ERR" | grep -q "assume role"; then
              echo "IAM role not ready. Waiting 20s before retry..."
              sleep 20
              break
            fi
            exit 1
          fi
          sleep 15
        done
      done
      echo "ERROR: Glue job failed after 3 attempts"
      exit 1
    EOT
  }

  depends_on = [aws_glue_job.data_generator, aws_s3_object.glue_script, aws_iam_role_policy_attachment.glue_etl_service]
}
