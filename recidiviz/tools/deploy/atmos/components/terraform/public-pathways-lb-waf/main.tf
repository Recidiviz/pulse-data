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

resource "google_project_service" "compute" {
  project            = var.project_id
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_compute_global_network_endpoint" "public-pathways-endpoint" {
  global_network_endpoint_group = google_compute_global_network_endpoint_group.public-pathways-neg.name
  fqdn       = var.project_id == "public-pathways-production" ? "public-pathways-production.web.app" : "public-pathways-staging.web.app"
  port       = 443
}

resource "google_compute_global_network_endpoint_group" "public-pathways-neg" {
  depends_on = [google_project_service.compute]

  name                  = "public-pathways-neg"
  project               = var.project_id
  default_port          = 443
  network_endpoint_type = "INTERNET_FQDN_PORT"
}

module "load_balancer" {
  source     = "../vendor/lb-http"
  depends_on = [google_project_service.compute]

  name    = "public-pathways-lb"
  project = var.project_id

  backends = {
    default = {
      groups = [
        {
          group = google_compute_global_network_endpoint_group.public-pathways-neg.id
        }
      ]

      enable_cdn      = false
      protocol        = "HTTPS"
      security_policy = google_compute_security_policy.public-pathways-waf-policy.id

      log_config = {
        enable      = true
        sample_rate = 1.0
      }

    }
  }

  ssl                             = true
  managed_ssl_certificate_domains = var.managed_ssl_certificate_domains
  https_redirect                  = true
}
