# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

resource "google_cloud_scheduler_job" "update_long_term_backups" {
  name = "update-long-term-backups"
  # Runs at a time when it's unlikely someone will be running the flashing checklist, to avoid
  # 'Operation failed because another operation was already in progress' errors.
  schedule         = "0 23 * * 1" # Every Monday 23:00
  description      = "Create new long-term backup and delete oldest long-term backup"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "2.500s"
    max_doublings        = 5
  }

  http_target {
    uri         = "${local.application_data_import_url}/backup_manager/update_long_term_backups"
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.application_data_import_cloud_run.email
    }
  }
}


resource "google_cloud_scheduler_job" "hydrate_admin_panel_cache" {
  name             = "hydrate-admin-panel-cache"
  schedule         = "*/15 * * * *" # Every 15 minutes
  description      = "[Admin Panel] Hydrate cache"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "30s"
    max_doublings        = 5
  }

  # when this cron job runs, create and run a Batch job
  http_target {
    http_method = "POST"
    uri         = "https://${var.us_central_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.admin_panel_hydrate_cache.name}:run"

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.admin_panel_cloud_run.email
    }
  }
}

locals {
  # Found at https://console.cloud.google.com/apis/credentials (IAP-admin-panel-load-balancer-backend-default)
  cloud_run_iap_client = local.is_production ? "688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.googleusercontent.com" : "984160736970-4vg3gpqmskvpkhqim39b8kp8e4ommu94.apps.googleusercontent.com"
}
