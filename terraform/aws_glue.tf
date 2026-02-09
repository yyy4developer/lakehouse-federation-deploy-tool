# =============================================================================
# AWS Glue Catalog Database & Tables
# Master data tables backed by CSV files on S3
# =============================================================================

resource "aws_glue_catalog_database" "factory_master" {
  name        = local.glue_database_name
  description = "Factory master data for Lakehouse Federation demo"
}

# -----------------------------------------------------------------------------
# Table: sensors (20 rows) - Sensor metadata
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "sensors" {
  database_name = aws_glue_catalog_database.factory_master.name
  name          = "sensors"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"         = "csv"
    "skip.header.line.count" = "1"
    "typeOfData"             = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.glue_data.id}/factory_master/sensors/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        "separatorChar" = ","
        "quoteChar"     = "\""
      }
    }

    columns {
      name = "sensor_id"
      type = "int"
    }
    columns {
      name = "sensor_name"
      type = "string"
    }
    columns {
      name = "sensor_type"
      type = "string"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name = "location"
      type = "string"
    }
    columns {
      name = "installed_date"
      type = "string"
    }
  }

  depends_on = [aws_s3_object.sensors_csv]
}

# -----------------------------------------------------------------------------
# Table: machines (10 rows) - Machine metadata
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "machines" {
  database_name = aws_glue_catalog_database.factory_master.name
  name          = "machines"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"         = "csv"
    "skip.header.line.count" = "1"
    "typeOfData"             = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.glue_data.id}/factory_master/machines/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        "separatorChar" = ","
        "quoteChar"     = "\""
      }
    }

    columns {
      name = "machine_id"
      type = "int"
    }
    columns {
      name = "machine_name"
      type = "string"
    }
    columns {
      name = "production_line"
      type = "string"
    }
    columns {
      name = "factory"
      type = "string"
    }
    columns {
      name = "status"
      type = "string"
    }
  }

  depends_on = [aws_s3_object.machines_csv]
}
