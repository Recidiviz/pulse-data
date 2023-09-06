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

locals {
  spotlight_dashboards = var.project_id == "recidiviz-123" ? {
    nd = "dashboard.docr.nd.gov"
    pa = "dashboard.cor.pa.gov"
    } : {
    nd = "us-nd.spotlight-staging.recidiviz.org"
    pa = "us-pa.spotlight-staging.recidiviz.org"
  }
}

resource "google_monitoring_uptime_check_config" "spotlight" {
  display_name = format("spotlight-%s", each.key)
  timeout      = "60s"
  for_each     = local.spotlight_dashboards

  http_check {
    path           = "/"
    request_method = "GET"
    port           = "443"
    use_ssl        = true
    validate_ssl   = true
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = each.value
    }
  }

  content_matchers {
    content = "\"Recidiviz\""
  }

  checker_type = "STATIC_IP_CHECKERS"
}


resource "google_monitoring_uptime_check_config" "dashboard" {
  display_name = "dashboard"
  timeout      = "60s"

  http_check {
    path           = "/"
    request_method = "GET"
    port           = "443"
    use_ssl        = true
    validate_ssl   = true
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = var.project_id == "recidiviz-123" ? "dashboard.recidiviz.org" : "dashboard-staging.recidiviz.org"
    }
  }

  content_matchers {
    content = "\"Recidiviz Dashboard\""
  }

  checker_type = "STATIC_IP_CHECKERS"
}


resource "google_monitoring_uptime_check_config" "case-triage" {
  display_name = "case-triage"
  timeout      = "60s"

  http_check {
    path           = "/health"
    request_method = "GET"
    port           = "443"
    use_ssl        = true
    validate_ssl   = true
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = var.project_id == "recidiviz-123" ? "app.recidiviz.org" : "app-staging.recidiviz.org"
    }
  }

  checker_type = "STATIC_IP_CHECKERS"
}


resource "google_monitoring_uptime_check_config" "marketing-website" {
  display_name = "marketing-website"
  timeout      = "60s"
  count        = var.project_id == "recidiviz-123" ? 1 : 0

  http_check {
    path           = "/"
    request_method = "GET"
    port           = "443"
    use_ssl        = true
    validate_ssl   = true
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = "recidiviz.org"
    }
  }


  content_matchers {
    content = "\"Recidiviz\""
  }

  checker_type = "STATIC_IP_CHECKERS"
}
