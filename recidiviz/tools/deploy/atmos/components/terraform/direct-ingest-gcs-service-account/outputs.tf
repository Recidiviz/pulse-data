output "service_account_email" {
  description = "Email of the service account that was created."
  value       = google_service_account.direct_ingest_service_account.email
}
