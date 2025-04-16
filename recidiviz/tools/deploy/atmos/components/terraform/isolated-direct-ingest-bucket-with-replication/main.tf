
locals {
  environment_to_project_id = tomap({
    staging = "recidiviz-staging"
    prod    = "recidiviz-123"
  })

  lower_state_code = replace(lower(var.state_code), "_", "-")
}

# Required permissions from https://cloud.google.com/storage-transfer/docs/cross-bucket-replication#grant-required-roles
data "google_storage_transfer_project_service_account" "default" {}

resource "google_project_iam_member" "data_transfer_sa_pubsub_editor" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_project_iam_member" "data_transfer_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

data "google_storage_project_service_account" "gcs_account" {}

resource "google_project_iam_member" "google_storage_sa_pubsub_editor" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

module "gcs_bucket" {
  source = "../vendor/cloud-storage-bucket"

  project_id = var.project_id
  location   = var.region
  prefix     = var.project_id
  names      = ["raw-files"]
  logging = {
    log_bucket = "${var.project_id}-gcs-object-logs"
  }
  versioning = {
    "raw-files" = true
  }
  storage_class = "STANDARD"
  lifecycle_rules = [{
    action = {
      type = "Delete"
    }
    condition = {
      age = 30
    }
  }]
}

resource "google_project_iam_audit_config" "project" {
  project = var.project_id
  service = "storage.googleapis.com"
  audit_log_config {
    log_type = "DATA_WRITE"
  }
}

resource "google_storage_bucket_iam_member" "project-bucket-writer" {
  for_each = local.environment_to_project_id

  bucket = "${each.value}-direct-ingest-state-${local.lower_state_code}"
  role   = "roles/storage.legacyBucketWriter"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_transfer_job" "copy_to_project" {
  for_each    = local.environment_to_project_id
  description = "Copies ${var.state_code} data to ${each.key}."

  replication_spec {
    dynamic "object_conditions" {
      for_each = (
      var.replication_spec_include_prefixes_file_path != null || var.replication_spec_exclude_prefixes_file_path != null
      ) ? [1] : []

      content {
        include_prefixes = (
        var.replication_spec_include_prefixes_file_path != null
        ? split("\n", trimspace(file(var.replication_spec_include_prefixes_file_path)))
        : null
        )
        exclude_prefixes = (
        var.replication_spec_exclude_prefixes_file_path != null
        ? split("\n", trimspace(file(var.replication_spec_exclude_prefixes_file_path)))
        : null
        )
      }
    }

    gcs_data_source {
      bucket_name = module.gcs_bucket.name
    }
    gcs_data_sink {
      bucket_name = "${each.value}-direct-ingest-state-${local.lower_state_code}"
    }
  }

  logging_config {
    log_actions = [
      "COPY",
      "DELETE"
    ]
    log_action_states = [
      "SUCCEEDED",
      "FAILED"
    ]
  }

  depends_on = [google_project_iam_member.data_transfer_sa_pubsub_editor, google_project_iam_member.data_transfer_sa_storage_admin, google_project_iam_member.google_storage_sa_pubsub_editor, google_storage_bucket_iam_member.project-bucket-writer]
}
