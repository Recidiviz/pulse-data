# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
# Daily snapshot of every Linear project into linear_snapshots.linear_project_snapshots,
# run by recidiviz/entrypoints/linear_project_snapshot.py.

# Service account for the Linear project snapshot Cloud Run job.
resource "google_service_account" "linear_project_snapshot" {
  account_id   = "linear-project-snapshot"
  display_name = "Linear Project Snapshot Cloud Run Service Account"
  description  = <<EOT
Runtime identity for the daily Linear project snapshot Cloud Run job. Least-privilege:
BigQuery job creation, write access to the linear_snapshots dataset only, and read
access to the job's two Linear OAuth secrets only. Managed in Terraform.
EOT
}

resource "google_project_iam_member" "linear_project_snapshot_runtime_iam" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.linear_project_snapshot.email}"
}

# Load-job access to the snapshot table's dataset only.
resource "google_bigquery_dataset_iam_member" "linear_project_snapshot_dataset_editor" {
  project    = var.project_id
  dataset_id = module.terraform_managed_bigquery_dataset["linear_snapshots"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.linear_project_snapshot.email}"
}

# Hold the client ID and client secret of the "Launch Momentum Snapshot" Linear
# OAuth application, which can read every team. Terraform creates the secrets
# without values; add them as secret versions by hand.
resource "google_secret_manager_secret" "linear_project_snapshot_oauth" {
  for_each = toset([
    "linear_project_snapshot_oauth_client_id",
    "linear_project_snapshot_oauth_client_secret",
  ])
  secret_id = each.key
  # The gcp.resourceLocations org policy allows US locations only, and it
  # rejects auto replication because auto replication counts as global.
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
      replicas {
        location = "us-east1"
      }
    }
  }
}

resource "google_secret_manager_secret_iam_member" "linear_project_snapshot_secret_access" {
  for_each  = google_secret_manager_secret.linear_project_snapshot_oauth
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.linear_project_snapshot.email}"
}

resource "google_cloud_run_v2_job" "linear_project_snapshot" {
  name     = "linear-project-snapshot"
  location = var.us_central_region
  provider = google-beta

  template {
    task_count = 1
    template {
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      service_account       = google_service_account.linear_project_snapshot.email
      max_retries           = 3
      timeout               = "600s"
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["uv"]
        # --no-sync since the image already contains locked dependencies.
        args = ["run", "--no-sync", "python", "-m", "recidiviz.entrypoints.linear_project_snapshot"]
        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

# Runs fail with a clear error, writing nothing, until both Linear OAuth secrets
# have values.
resource "google_cloud_scheduler_job" "linear_project_snapshot" {
  name             = "linear-project-snapshot"
  schedule         = "0 6 * * *" # Every day at 6 am PT
  time_zone        = "US/Pacific"
  paused           = false
  description      = "[Linear] Trigger the daily Linear project snapshot Cloud Run job"
  attempt_deadline = "320s"

  retry_config {
    min_backoff_duration = "15s"
    max_doublings        = 5
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.us_central_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.linear_project_snapshot.name}:run"

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.cloud_scheduler.email
    }
  }
}

# Job-level run.invoker for the Scheduler identity, not a project-wide grant.
resource "google_cloud_run_v2_job_iam_member" "linear_project_snapshot_scheduler_invoker" {
  provider = google-beta
  project  = var.project_id
  location = var.us_central_region
  name     = google_cloud_run_v2_job.linear_project_snapshot.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_scheduler.email}"
}
