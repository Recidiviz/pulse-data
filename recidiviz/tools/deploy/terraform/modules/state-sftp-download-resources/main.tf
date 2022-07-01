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
data "google_app_engine_default_service_account" "default" {}
module "sftp-storage-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = local.direct_ingest_sftp_str
  location    = var.region

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 14
      }
    }
  ]
}

module "sftp-download-queue" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_sftp_str}-queue"
  region                    = var.region
  max_dispatches_per_second = 5
  max_retry_attempts        = 5
  logging_sampling_ratio    = 1.0
}

resource "google_cloud_scheduler_job" "sftp-cloud-scheduler" {
  # Only for Michigan in production do we configure this
  count            = (var.project_id == "recidiviz-123" && var.state_code == "US_MI") ? 1 : 0
  name             = "${local.lower_state_code}-sftp-cron-job"
  schedule         = "0 */3 * * *" # Every 3 hours
  description      = "${var.state_code} SFTP cloud scheduler cron job"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes
  region           = "us-east1"

  retry_config {
    min_backoff_duration = "30s"
    max_doublings        = 5
  }

  http_target {
    uri         = "https://${var.project_id}.appspot.com/direct/handle_sftp_files?region=${lower(var.state_code)}"
    http_method = "POST"

    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      # Only for production
      audience = "688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.googleusercontent.com"
    }
  }
}
