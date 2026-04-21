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


locals {
  identity_service_load_balancer_name = "identity-service-load-balancer"
}

# Contains YAML list of IAM policy members that should have access to the Identity Service
data "google_secret_manager_secret_version" "iam_identity_service_access" {
  secret = "iam_identity_service_access"
}

resource "google_service_account" "identity_service_cloud_run" {
  account_id   = "identity-service-cr"
  display_name = "Identity Service Cloud Run Service Account"
  description  = <<EOT
Service Account that acts as the identity for the Identity Service Cloud Run service.
The account and its IAM policies are managed in Terraform.
EOT
}

resource "google_project_iam_member" "identity_service_iam" {
  for_each = toset([
    "roles/secretmanager.secretAccessor",
    "roles/cloudsql.client",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/cloudtrace.agent",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.identity_service_cloud_run.email}"
}

resource "google_cloud_run_service" "identity_service" {
  name     = "identity-service"
  location = var.us_central_region
  project  = var.project_id

  metadata {
    annotations = {
      # Only exposed to internal traffic and our application load balancer
      "run.googleapis.com/ingress" : "internal-and-cloud-load-balancing"
    }
  }

  template {
    spec {
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["sh"]
        args = [
          "-c",
          join(" ", [
            "uv",
            "run",
            "gunicorn",
            "-c",
            "gunicorn.conf.py",
            "-b",
            ":$PORT",
            "--log-file=-",
            # TODO(#71768): Update this entrypoint when the Identity Service Flask app is implemented
            "recidiviz.identity_service.server:app",
          ])
        ]

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
      service_account_name = google_service_account.identity_service_cloud_run.email
    }

    metadata {
      annotations = {
        "run.googleapis.com/cloudsql-instances"   = module.identity_service_database.connection_name
        "run.googleapis.com/vpc-access-connector" = google_vpc_access_connector.us_central_redis_vpc_connector.id
        "run.googleapis.com/vpc-access-egress"    = "private-ranges-only"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}

resource "google_compute_region_network_endpoint_group" "identity_service_serverless_neg" {
  provider              = google-beta
  name                  = "identity-service-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.us_central_region
  cloud_run {
    service = google_cloud_run_service.identity_service.name
  }
}

module "identity_service_load_balancer" {
  source  = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version = "~> 12.0.0"
  name    = local.identity_service_load_balancer_name
  project = var.project_id

  ssl                             = true
  ssl_policy                      = google_compute_ssl_policy.restricted-ssl-policy.name
  managed_ssl_certificate_domains = local.is_production ? ["identity-service.recidiviz.org"] : ["identity-service-staging.recidiviz.org"]
  https_redirect                  = true

  backends = {
    default = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.identity_service_serverless_neg.id
        }
      ]
      enable_cdn              = false
      security_policy         = google_compute_security_policy.recidiviz-waf-policy.id
      custom_request_headers  = null
      custom_response_headers = null

      iap_config = {
        enable               = true
        oauth2_client_id     = data.google_secret_manager_secret_version.iap_client_id.secret_data
        oauth2_client_secret = data.google_secret_manager_secret_version.iap_client_secret.secret_data
      }

      log_config = {
        enable      = true
        sample_rate = 1
      }
    }
  }
}

data "google_iam_policy" "identity_service_iap" {
  binding {
    role    = "roles/iap.httpsResourceAccessor"
    members = yamldecode(data.google_secret_manager_secret_version.iam_identity_service_access.secret_data)
  }
}

resource "google_iap_web_backend_service_iam_policy" "identity_service_policy" {
  project             = var.project_id
  web_backend_service = format("%s-backend-default", local.identity_service_load_balancer_name)
  policy_data         = data.google_iam_policy.identity_service_iap.policy_data
  depends_on = [
    module.identity_service_load_balancer
  ]
}
