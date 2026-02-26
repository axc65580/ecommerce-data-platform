output "bronze_bucket_name" {
  description = "Bronze layer S3 bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  description = "Silver layer S3 bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  description = "Gold layer S3 bucket name"
  value       = aws_s3_bucket.gold.bucket
}

output "kafka_bootstrap_brokers" {
  description = "MSK Kafka bootstrap broker string"
  value       = aws_msk_cluster.ecommerce.bootstrap_brokers
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.ecommerce.name
}
