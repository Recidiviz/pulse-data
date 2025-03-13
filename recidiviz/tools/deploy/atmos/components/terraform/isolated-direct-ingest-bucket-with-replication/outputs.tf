output "bucket_name" {
  description = "Name of the bucket that was created."
  value       = module.gcs_bucket.name
}
