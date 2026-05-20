resource "aws_glue_job" "great_expectation" {
  name     = "Proyecto_great_expectation"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://aws-glue-assets-444655873165-us-east-1/scripts/Proyecto_great_expectation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                   = "s3://${var.temp_bucket}/temporary/"
    "--additional-python-modules" = "great-expectations==0.17.21,openpyxl"
    "--job-language"              = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.0"
  timeout           = 10
  tags              = var.tags
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "Proyecto_bronze_to_silver"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://aws-glue-assets-444655873165-us-east-1/scripts/Proyecto_bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                   = "s3://${var.temp_bucket}/temporary/"
    "--additional-python-modules" = "openpyxl"
    "--job-language"              = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.0"
  timeout           = 10
  tags              = var.tags
}

resource "aws_glue_job" "silver_to_gold" {
  name     = "Proyecto_silver_to_gold"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://aws-glue-assets-444655873165-us-east-1/scripts/Proyecto_silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"      = "s3://${var.temp_bucket}/temporary/"
    "--job-language" = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.0"
  timeout           = 10
  tags              = var.tags
}