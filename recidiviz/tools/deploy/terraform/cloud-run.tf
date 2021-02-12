# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

# Create a new service account for all things Cloud Run
resource "google_service_account" "cloud_run" {
  account_id   = "cloud-run-service-account"
  display_name = "Cloud Run Service Account"
}

resource "google_project_iam_member" "cloud_run_admin" {
  role   = "roles/run.admin"
  member = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_secret_accessor" {
  role   = "roles/secretmanager.secretAccessor"
  member = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_cloud_sql" {
  role   = "roles/cloudsql.client"
  member = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_gcs_access" {
  role   = google_project_iam_custom_role.gcs-object-and-bucket-viewer.name
  member = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_log_writer" {
  role   = "roles/logging.logWriter"
  member = "serviceAccount:${google_service_account.cloud_run.email}"
}

# Initializes actual service
resource "google_cloud_run_service" "case-triage" {
  name     = "case-triage-web"
  location = var.region

  template {
    spec {
      containers {
        image   = "us.gcr.io/recidiviz-staging/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.case_triage.server:app"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }

        resources {
          limits = {
            memory = "1024Mi"
          }
        }
      }

      service_account_name = google_service_account.cloud_run.email
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"      = 1
        "autoscaling.knative.dev/maxScale"      = var.max_case_triage_instances
        "run.googleapis.com/cloudsql-instances" = local.joined_connection_string
      }

      name = "case-triage-web-${replace(var.docker_image_tag, ".", "-")}"
    }
  }

  metadata {
    annotations = {
      "run.googleapis.com/launch-stage" = "BETA"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = false
}

# Sets up public access
resource "google_cloud_run_service_iam_member" "public-access" {
  location = google_cloud_run_service.case-triage.location
  project  = google_cloud_run_service.case-triage.project
  service  = google_cloud_run_service.case-triage.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
