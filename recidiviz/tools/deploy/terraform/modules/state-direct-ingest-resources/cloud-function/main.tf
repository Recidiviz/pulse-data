variable "bucket_name" {
  type = string
}

variable "state_code" {
  type = string
}

variable "ingest_instance" {
  type    = string
  default = "PRIMARY"
}

variable "git_hash" {
  type = string
}

variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "dry_run" {
  type = bool
}

locals {
  normalization_function_name = "ingest_filename_normalization_${var.state_code}${var.ingest_instance != "PRIMARY" ? "_${lower(var.ingest_instance)}" : ""}"
  zip_function_name = "ingest_zipfile_handler"
}

data "google_storage_project_service_account" "default" {
}

resource "google_project_iam_member" "gcs_pubsub_publishing" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.default.email_address}"
}

resource "google_project_iam_member" "invoking" {
  project    = var.project_id
  role       = "roles/run.invoker"
  member     = "serviceAccount:${var.service_account_email}"
  depends_on = [google_project_iam_member.gcs_pubsub_publishing]
}

resource "google_project_iam_member" "event_receiving" {
  project    = var.project_id
  role       = "roles/eventarc.eventReceiver"
  member     = "serviceAccount:${var.service_account_email}"
  depends_on = [google_project_iam_member.invoking]
}

resource "google_project_iam_member" "storage_admin" {
  project    = var.project_id
  role       = "roles/storage.objectAdmin"
  member     = "serviceAccount:${var.service_account_email}"
  depends_on = [google_project_iam_member.event_receiving]
}

resource "google_cloudfunctions2_function" "filename_normalization" {
  depends_on = [
    google_project_iam_member.event_receiving,
    google_project_iam_member.storage_admin,
  ]
  name        = local.normalization_function_name
  location    = var.region
  description = "Normalize ingest file names for ${var.bucket_name}"
  build_config {
    runtime     = "python311"
    entry_point = "normalize_filename"
    environment_variables = {
      # Hacky workaround since source directory option is broken https://issuetracker.google.com/issues/248110968
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
      GOOGLE_FUNCTION_SOURCE = "recidiviz/cloud_functions/ingest_filename_normalization.py"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    trigger_region        = var.region # The trigger must be in the same location as the bucket
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = var.service_account_email
    event_filters {
      attribute = "bucket"
      value     = var.bucket_name
    }
  }

  service_config {
    service_account_email = var.service_account_email
    max_instance_count = 50
    available_memory   = "512M"
    timeout_seconds    = 540
    environment_variables = {
      PYTHONPATH      = "/workspace" # directory recidiviz/ lives in
      ZIPFILE_HANDLER_FUNCTION_URL = "https://us-central1-${var.project_id}.cloudfunctions.net/ingest_zipfile_handler"
      DRY_RUN         = var.dry_run
    }
  }
}
