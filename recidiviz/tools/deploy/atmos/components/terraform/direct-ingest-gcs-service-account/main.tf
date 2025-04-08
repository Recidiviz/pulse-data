locals {
  lower_state_code            = replace(lower(var.state_code), "_", "-")
}

resource "google_service_account" "direct_ingest_service_account" {
  account_id   = "${local.lower_state_code}-raw-data-transfer"
  display_name = "A service account for ${var.state_code} raw data transfer to the ${var.project_id} project."
}


resource "google_project_iam_custom_role" "direct-ingest-data-writer-role" {
  role_id     = "directIngestDataWriter"
  title       = "State Admin Role"
  description = "Role that gives external state agencies permissions to upload files with state data to GCS and to list all buckets in the given project."
  permissions = [
    "storage.buckets.get",
    "storage.buckets.list",
    "storage.objects.create",
    "storage.objects.get",
    "storage.objects.list",
    "storage.objects.delete"
  ]
}

# Give the service account the ability to list all buckets / GCS objects in the whole project
resource "google_project_iam_member" "gcs-buckets-viewer" {
  project= var.project_id
  role   = google_project_iam_custom_role.direct-ingest-data-writer-role.name
  member = "serviceAccount:${google_service_account.direct_ingest_service_account.email}"
}

# Give the service account the ability to create objects in the ingest bucket
resource "google_storage_bucket_iam_member" "ingest-bucket-writer" {
  role   = google_project_iam_custom_role.direct-ingest-data-writer-role.name
  bucket = var.ingest_bucket_name
  member = "serviceAccount:${google_service_account.direct_ingest_service_account.email}"
}
