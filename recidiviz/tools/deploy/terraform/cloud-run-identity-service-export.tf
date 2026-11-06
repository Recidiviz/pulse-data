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
# Identity Service export (OBT-51978): the IdentityServiceExportEntrypoint run
# as a scheduled Cloud Run job. Reads the Identity Service's Postgres state and
# rewrites the identity_service_export BigQuery dataset.

# Service account for the Identity Service export Cloud Run job.
resource "google_service_account" "identity_service_export" {
  account_id   = "identity-service-export"
  display_name = "Identity Service Export Cloud Run Service Account"
  description  = <<EOT
Runtime identity for the Identity Service export Cloud Run job. Least-privilege:
read access to the Identity Service's Cloud SQL database and write access to the
identity_service_export BigQuery dataset only. Managed in Terraform.
EOT
}

resource "google_project_iam_member" "identity_service_export_runtime_iam" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/cloudsql.client",
    "roles/cloudtrace.agent",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.identity_service_export.email}"
}

# Write access to the export dataset only. The dataset is created by the
# source-table update process, not Terraform; its name matches
# IDENTITY_SERVICE_EXPORT_DATASET_ID in
# recidiviz/source_tables/identity_service_export_source_tables.py.
resource "google_bigquery_dataset_iam_member" "identity_service_export_dataset_editor" {
  project    = var.project_id
  dataset_id = "identity_service_export"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.identity_service_export.email}"
}

# Secret-level access: only the Identity Service database credentials, not all
# project secrets. These are the secrets SQLAlchemyEngineManager reads for
# SchemaType.IDENTITY (secret prefix identity_service; see
# recidiviz/persistence/database/base_engine_manager.py).
resource "google_secret_manager_secret_iam_member" "identity_service_export_secret_access" {
  for_each = toset([
    "identity_service_db_user",
    "identity_service_db_password",
    "identity_service_db_host",
    "identity_service_cloudsql_instance_id",
  ])
  secret_id = each.key
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.identity_service_export.email}"
}

resource "google_cloud_run_v2_job" "identity_service_export" {
  name     = "identity-service-export"
  location = var.us_central_region
  provider = google-beta

  template {
    task_count = 1
    template {
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      service_account       = google_service_account.identity_service_export.email
      max_retries           = 3
      # A full export reads all identities in one transaction and reloads every
      # table; 30 minutes is generous while only test tenants are imported.
      timeout = "1800s"
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["uv"]
        # --no-sync since the image already contains locked dependencies.
        args = [
          "run",
          "--no-sync",
          "python",
          "-m",
          "recidiviz.entrypoints.entrypoint_executor",
          "--entrypoint=IdentityServiceExportEntrypoint",
        ]
        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
        # This is the expected path for Cloud SQL unix sockets in GCP runtimes:
        # https://cloud.google.com/sql/docs/postgres/connect-run#public-ip-default_1
        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "1Gi"
          }
        }
      }
      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [
            module.identity_service_database.connection_name,
          ]
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

# TODO(OBT-51155): Lift the staging gate once the Identity Service is deployed
# in production. Until then the job exists in production for manual runs, but
# nothing schedules it.
resource "google_cloud_scheduler_job" "identity_service_export" {
  count            = var.project_id == "recidiviz-staging" ? 1 : 0
  name             = "identity-service-export"
  schedule         = "0 0 * * *" # Every day at midnight UTC
  time_zone        = "Etc/UTC"
  paused           = false
  description      = "[Identity Service] Trigger the daily export Cloud Run job"
  attempt_deadline = "320s"

  retry_config {
    min_backoff_duration = "15s"
    max_doublings        = 5
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.us_central_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.identity_service_export.name}:run"

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.cloud_scheduler.email
    }
  }
}

# Job-level run.invoker for the Scheduler identity, not a project-wide grant.
resource "google_cloud_run_v2_job_iam_member" "identity_service_export_scheduler_invoker" {
  provider = google-beta
  project  = var.project_id
  location = var.us_central_region
  name     = google_cloud_run_v2_job.identity_service_export.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_scheduler.email}"
}
