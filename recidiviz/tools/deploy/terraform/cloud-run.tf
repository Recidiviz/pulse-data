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

# Create a new service account for Case Triage Cloud Run
resource "google_service_account" "cloud_run" {
  account_id   = "cloud-run-service-account"
  display_name = "Cloud Run Service Account"
}

resource "google_project_iam_member" "cloud_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_cloud_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_gcs_access" {
  project = var.project_id
  role    = google_project_iam_custom_role.gcs-object-and-bucket-viewer.name
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

# Use existing Justice Counts service account for Justice Counts Cloud Run
data "google_service_account" "justice_counts_cloud_run" {
  account_id = "jstc-counts-spotlights-staging"

  # TODO(#11710): Remove staging-only check when we create production environment
  count = var.project_id == "recidiviz-123" ? 0 : 1
}

# Env vars from secrets
data "google_secret_manager_secret_version" "segment_write_key" { secret = "case_triage_segment_backend_key" }

# Initializes Case Triage Cloud Run service
resource "google_cloud_run_service" "case-triage" {
  name     = "case-triage-web"
  location = var.region

  template {
    spec {
      containers {
        image   = "us.gcr.io/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.case_triage.server:app"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }

        env {
          name  = "APP_URL"
          value = var.project_id == "recidiviz-123" ? "https://app.recidiviz.org" : "https://app-staging.recidiviz.org"
        }

        env {
          name  = "DASHBOARD_URL"
          value = "https://dashboard.recidiviz.org"
        }

        env {
          name  = "SEGMENT_WRITE_KEY"
          value = data.google_secret_manager_secret_version.segment_write_key.secret_data
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "1024Mi"
          }
        }
      }

      service_account_name = google_service_account.cloud_run.email
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"        = 1
        "autoscaling.knative.dev/maxScale"        = var.max_case_triage_instances
        "run.googleapis.com/cloudsql-instances"   = local.joined_connection_string
        "run.googleapis.com/vpc-access-connector" = google_vpc_access_connector.us_central_redis_vpc_connector.name
        "run.googleapis.com/vpc-access-egress"    = "private-ranges-only"
      }

      name = "case-triage-web-${replace(var.docker_image_tag, ".", "-")}"
    }
  }

  metadata {
    annotations = {
      "run.googleapis.com/ingress"        = "all"
      "run.googleapis.com/ingress-status" = "all"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = false
}

# Initializes Justice Counts Cloud Run service
# TODO(#11710): Remove staging-only check when we create production environment
resource "google_cloud_run_service" "justice-counts" {
  name     = "justice-counts-web"
  location = var.region

  # TODO(#11710): Remove staging-only check when we create production environment
  count = var.project_id == "recidiviz-123" ? 0 : 1

  template {
    spec {
      containers {
        image   = "us.gcr.io/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.justice_counts.control_panel.server:create_app()"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
      }


      service_account_name = data.google_service_account.justice_counts_cloud_run[count.index].email
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"      = 1
        "autoscaling.knative.dev/maxScale"      = var.max_justice_counts_instances
        "run.googleapis.com/cloudsql-instances" = module.justice_counts_database.connection_name
      }

      name = "justice-counts-web-${replace(var.docker_image_tag, ".", "-")}"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = false
}

# By default, Cloud Run services are private and secured by IAM. 
# The blocks below set up public access so that anyone (e.g. our frontends)
# can invoke the services through an HTTP endpoint.
resource "google_cloud_run_service_iam_member" "public-access" {
  location = google_cloud_run_service.case-triage.location
  project  = google_cloud_run_service.case-triage.project
  service  = google_cloud_run_service.case-triage.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_service_iam_member" "justice-counts-public-access" {
  # TODO(#11710): Remove staging-only check when we create production environment
  count = var.project_id == "recidiviz-123" ? 0 : 1

  location = google_cloud_run_service.justice-counts[count.index].location
  project  = google_cloud_run_service.justice-counts[count.index].project
  service  = google_cloud_run_service.justice-counts[count.index].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Setting up load balancer
# Drawn from https://github.com/terraform-google-modules/terraform-google-lb-http/blob/master/examples/cloudrun/main.tf
resource "google_compute_region_network_endpoint_group" "serverless_neg" {
  provider              = google-beta
  name                  = "unified-product-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region
  cloud_run {
    service = google_cloud_run_service.case-triage.name
  }
}

resource "google_compute_ssl_policy" "modern-ssl-policy" {
  name            = "modern-ssl-policy"
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"
}

module "unified-product-load-balancer" {
  source  = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version = "~> 6.2.0"
  name    = "unified-product-lb"
  project = var.project_id

  ssl                             = true
  ssl_policy                      = google_compute_ssl_policy.modern-ssl-policy.name
  managed_ssl_certificate_domains = local.is_production ? ["app-prod.recidiviz.org", "app.recidiviz.org"] : ["app-staging.recidiviz.org"]
  https_redirect                  = true

  backends = {
    default = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.serverless_neg.id
        }
      ]
      enable_cdn              = true
      security_policy         = null
      custom_request_headers  = null
      custom_response_headers = null

      iap_config = {
        enable               = false
        oauth2_client_id     = ""
        oauth2_client_secret = ""
      }
      log_config = {
        enable      = true
        sample_rate = null
      }
    }
  }
}
