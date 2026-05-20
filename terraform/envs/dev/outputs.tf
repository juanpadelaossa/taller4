output "bronze_bucket_name" {
  value = data.aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  value = data.aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  value = data.aws_s3_bucket.gold.bucket
}