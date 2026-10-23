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
    content = "Recidiviz"
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
    content = "Recidiviz Dashboard"
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
    content = "Recidiviz"
  }

  checker_type = "STATIC_IP_CHECKERS"
}


# Hosts outside this project that page on-call through the "Uptime Check URL - DOWN"
# policy and warn ahead of cert expiry through the "SSL certificate expiring" policy,
# both in the data-platform-alerting component. Each check validates the TLS
# certificate, so an expired or mismatched certificate fails the check even when the
# app behind it is healthy. status is the response code a healthy host returns for
# path without credentials: IAP-protected hosts redirect to Google sign-in (302), and
# the sftpgo admin consoles sit behind a Cloud Armor allowlist that rejects the
# uptime checkers (403) only after the TLS handshake completes.
locals {
  external_uptime_checks = var.project_id == "recidiviz-123" ? {
    "sftpgo-admin-us-nc"  = { host = "sftpgo-admin-us-nc.recidiviz.org", path = "/", status = 403 }
    "sftpgo-admin-us-nyc" = { host = "sftpgo-admin-us-nyc.recidiviz.org", path = "/", status = 403 }
    "sftpgo-admin-us-nv"  = { host = "sftpgo-admin-us-nv.recidiviz.org", path = "/", status = 403 }
    "sftpgo-admin-us-tn"  = { host = "sftpgo-admin-us-tn.recidiviz.org", path = "/", status = 403 }
    "resource-api"        = { host = "resource.recidiviz.org", path = "/api/docs", status = 200 }
    "auth0-login"         = { host = "login.recidiviz.org", path = "/.well-known/openid-configuration", status = 200 }
    "app-prod"            = { host = "app-prod.recidiviz.org", path = "/health", status = 200 }
    "identity-service"    = { host = "identity-service.recidiviz.org", path = "/", status = 302 }
    "admin-panel"         = { host = "admin-panel-prod.recidiviz.org", path = "/", status = 302 }
    "dashboard-cdn"       = { host = "dashboard-cdn.recidiviz.org", path = "/", status = 200 }
    "housing"             = { host = "housing.recidiviz.org", path = "/", status = 200 }
    "housing-admin"       = { host = "housing-admin.recidiviz.org", path = "/", status = 302 }
    "planner"             = { host = "plan.recidiviz.org", path = "/", status = 307 }
    "planner-api"         = { host = "api.plan.recidiviz.org", path = "/health", status = 200 }
    "public-pathways"     = { host = "pathways.recidiviz.org", path = "/", status = 200 }
    "pathways-app"        = { host = "pathways-app.recidiviz.org", path = "/health", status = 200 }
    "opportunities-app"   = { host = "opportunities.app", path = "/", status = 200 }
    "opportunities-edovo" = { host = "opportunities.edovo.com", path = "/", status = 200 }
    } : {
    "resource-api" = { host = "resource-dev.recidiviz.org", path = "/api/docs", status = 200 }
  }
}

resource "google_monitoring_uptime_check_config" "external" {
  for_each = local.external_uptime_checks

  display_name = each.key
  timeout      = "60s"

  http_check {
    path           = each.value.path
    request_method = "GET"
    port           = "443"
    use_ssl        = true
    validate_ssl   = true

    accepted_response_status_codes {
      status_value = each.value.status
    }
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = each.value.host
    }
  }

  checker_type = "STATIC_IP_CHECKERS"
}
