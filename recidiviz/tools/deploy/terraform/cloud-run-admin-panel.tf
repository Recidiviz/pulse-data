# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
  admin_panel_load_balancer_name = "admin-panel-load-balancer"
}

# Contains YAML list of IAM policy members that should have access to the admin panel
data "google_secret_manager_secret_version" "iam_admin_panel_access" {
  secret = "iam_admin_panel_access"
}


# Contains the Identity Aware Proxy's OAuth2 client id
data "google_secret_manager_secret_version" "iap_client_id" {
  secret = "iap_client_id"
}


# Contains the Identity Aware Proxy's OAuth2 client secret
data "google_secret_manager_secret_version" "iap_client_secret" {
  secret = "iap_client_secret"
}


resource "google_cloud_run_service" "admin_panel" {
  name     = "admin-panel"
  location = var.region
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
        image   = "us.gcr.io/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.admin_panel.server:app"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "1024Mi"
          }
        }
      }
      service_account_name = google_service_account.admin_panel_cloud_run.email
    }

    metadata {
      annotations = {
        "run.googleapis.com/vpc-access-connector" : google_vpc_access_connector.us_central_redis_vpc_connector.name
        "autoscaling.knative.dev/maxScale" : 3
      }
    }

  }
}

resource "google_compute_region_network_endpoint_group" "admin_panel_serverless_neg" {
  provider              = google-beta
  name                  = "admin-panel-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region
  cloud_run {
    service = google_cloud_run_service.admin_panel.name
  }
}


module "admin_panel_load_balancer" {
  source  = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version = "~> 6.2.0"
  name    = local.admin_panel_load_balancer_name
  project = var.project_id

  ssl                             = true
  ssl_policy                      = google_compute_ssl_policy.restricted-ssl-policy.name
  managed_ssl_certificate_domains = local.is_production ? ["admin-panel-prod.recidiviz.org"] : ["admin-panel-staging.recidiviz.org"]
  https_redirect                  = true

  backends = {
    default = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.admin_panel_serverless_neg.id
        }
      ]
      enable_cdn      = false
      security_policy = google_compute_security_policy.recidiviz-waf-policy.id
      custom_request_headers = [
        "X-Client-Geo-Location: {client_region_subdivision}, {client_city}",
        "TLS_VERSION: {tls_version}",
        "TLS_CIPHER_SUITE: {tls_cipher_suite}",
        "CLIENT_ENCRYPTED: {client_encrypted}"
      ]
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

data "google_iam_policy" "admin_panel_iap" {
  binding {
    role    = "roles/iap.httpsResourceAccessor"
    members = yamldecode(data.google_secret_manager_secret_version.iam_admin_panel_access.secret_data)
  }
}

resource "google_iap_web_backend_service_iam_policy" "admin_panel_policy" {
  project             = var.project_id
  web_backend_service = format("%s-backend-default", local.admin_panel_load_balancer_name)
  policy_data         = data.google_iam_policy.admin_panel_iap.policy_data
  depends_on = [
    module.admin_panel_load_balancer
  ]
}
