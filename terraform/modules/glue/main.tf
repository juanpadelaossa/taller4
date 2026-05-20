resource "aws_glue_job" "great_expectations" {
  name     = "Great_Expectations"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://aws-glue-assets-444655873165-us-east-1/scripts/Great_Expectations.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                   = "s3://datalake-bronze-444655873165/temporary/"
    "--additional-python-modules" = "great-expectations==0.17.21"
    "--conf"                      = "spark.eventLog.rolling.enabled=true"
    "--job-language"              = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.0"
  timeout           = 10
  tags              = var.tags
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "bronze_to_silver"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://aws-glue-assets-444655873165-us-east-1/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"      = "s3://datalake-bronze-444655873165/temporary/"
    "--job-language" = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.0"
  timeout           = 10
  tags              = var.tags
}

resource "aws_glue_job" "silver_to_gold" {
  name     = "silver_to_gold"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://aws-glue-assets-444655873165-us-east-1/scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"      = "s3://datalake-bronze-444655873165/temporary/"
    "--job-language" = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.0"
  timeout           = 10
  tags              = var.tags
}