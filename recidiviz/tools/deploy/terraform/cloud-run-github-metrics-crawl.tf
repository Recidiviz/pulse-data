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
# Nightly crawl of pull request history from the GitHub GraphQL API into the
# github_api_raw tables, run by recidiviz/entrypoints/github_metrics_crawl.py.
#
# Each run reads the pull requests updated since the last one. A repo with no rows
# in github_api_raw.pull_requests, in a new project or just added to the
# allowlist, starts from 2025-05-01 and loads its history. That load can outlast
# the one-hour task timeout; a run that stops keeps the rows it wrote and the next
# run continues from them, so the load finishes over a few nights with no manual
# step. To finish it in one run, execute the job by hand with a longer timeout:
#
#   gcloud run jobs execute github-metrics-crawl --region us-central1 --task-timeout 24h

# Service account for the GitHub metrics crawl Cloud Run job.
resource "google_service_account" "github_metrics_crawl" {
  account_id   = "github-metrics-crawl"
  display_name = "GitHub Metrics Crawl Cloud Run Service Account"
  description  = <<EOT
Runtime identity for the nightly GitHub metrics crawl Cloud Run job. Least-privilege:
BigQuery job creation, write access to the github_api_raw dataset only, and read
access to the job's three GitHub App secrets only.
EOT
}

resource "google_project_iam_member" "github_metrics_crawl_runtime_iam" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.github_metrics_crawl.email}"
}

# Load-job and watermark-query access to the crawl's own dataset.
resource "google_bigquery_dataset_iam_member" "github_metrics_crawl_dataset_editor" {
  project    = var.project_id
  dataset_id = module.terraform_managed_bigquery_dataset["github_api_raw"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.github_metrics_crawl.email}"
}

# Hold the crawl's read-only GitHub App credential. The job mints one-hour
# installation tokens from it. Terraform creates the secrets without values; add
# the App id, installation id and private key as secret versions by hand.
resource "google_secret_manager_secret" "github_metrics_crawl_credentials" {
  for_each = toset([
    "github_metrics_crawl_app_id",
    "github_metrics_crawl_app_installation_id",
    "github_metrics_crawl_app_private_key",
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

resource "google_secret_manager_secret_iam_member" "github_metrics_crawl_secret_access" {
  for_each  = google_secret_manager_secret.github_metrics_crawl_credentials
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.github_metrics_crawl.email}"
}

resource "google_cloud_run_v2_job" "github_metrics_crawl" {
  name     = "github-metrics-crawl"
  location = var.us_central_region
  provider = google-beta

  template {
    task_count = 1
    template {
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      service_account       = google_service_account.github_metrics_crawl.email
      max_retries           = 1
      timeout               = "3600s"
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["uv"]
        # --no-sync since the image already contains locked dependencies.
        args = ["run", "--no-sync", "python", "-m", "recidiviz.entrypoints.github_metrics_crawl"]
        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "1Gi"
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

resource "google_cloud_scheduler_job" "github_metrics_crawl" {
  name             = "github-metrics-crawl"
  schedule         = "0 3 * * *" # Every day at 3 am UTC
  time_zone        = "Etc/UTC"
  paused           = false
  description      = "[Eng Metrics] Trigger the nightly GitHub metrics crawl Cloud Run job"
  attempt_deadline = "320s"

  retry_config {
    min_backoff_duration = "15s"
    max_doublings        = 5
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.us_central_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.github_metrics_crawl.name}:run"

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.cloud_scheduler.email
    }
  }
}

# Job-level run.invoker for the Scheduler identity, not a project-wide grant.
resource "google_cloud_run_v2_job_iam_member" "github_metrics_crawl_scheduler_invoker" {
  provider = google-beta
  project  = var.project_id
  location = var.us_central_region
  name     = google_cloud_run_v2_job.github_metrics_crawl.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_scheduler.email}"
}
