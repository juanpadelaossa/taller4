terraform {
  backend "s3" {
    bucket       = "datalake-terraform-state-444655873165"
    key          = "dev/renault/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

# Referenciar buckets existentes
data "aws_s3_bucket" "bronze" {
  bucket = "datalake-bronze-444655873165"
}

data "aws_s3_bucket" "silver" {
  bucket = "datalake-silver-444655873165"
}

data "aws_s3_bucket" "gold" {
  bucket = "datalake-gold-444655873165"
}

module "glue_job" {
  source = "../../modules/glue"

  project = var.project
  env     = var.env

  glue_role_arn = module.iam.glue_role_arn

  bronze_bucket = data.aws_s3_bucket.bronze.bucket
  silver_bucket = data.aws_s3_bucket.silver.bucket
  temp_bucket   = data.aws_s3_bucket.bronze.bucket

  tags = var.tags
}

module "iam" {
  source = "../../modules/iam"

  project = var.project
  env     = var.env

  bronze_bucket = data.aws_s3_bucket.bronze.bucket
  silver_bucket = data.aws_s3_bucket.silver.bucket
  gold_bucket   = data.aws_s3_bucket.gold.bucket
  temp_bucket   = data.aws_s3_bucket.bronze.bucket
}