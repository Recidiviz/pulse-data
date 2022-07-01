# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

# TODO(#4690): The direct-ingest-county cloud function is not included because its entry
# point is `direct_ingest_county`, which no longer exists in our codebase!!
# If we get approval to delete it, we will remove this comment. Otherwise,
# we will find the right endpoint and fold it into this file.

data "google_secret_manager_secret_version" "sendgrid_api_key" {
  secret = "sendgrid_api_key"
}

data "google_secret_manager_secret_version" "po_report_cdn_static_ip" {
  secret = "po_report_cdn_static_IP"
}


resource "google_cloudfunctions_function" "export_metric_view_data" {
  name    = "export_metric_view_data"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/${var.project_id}/topics/v1.export.view.data"
  }

  entry_point = "export_metric_view_data"
  environment_variables = {
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }

  # TODO(#4379): This should be removed when we have a more scalable solution
  # for preventing 499s in the metric export pipeline. Longer term, we want this
  # cloud function to issue an async request and return an id that can be queried
  # by another process.
  timeout = 540
}


resource "google_cloudfunctions_function" "trigger_post_deploy_cloudsql_to_bq_refresh_state" {
  name    = "trigger_post_deploy_cloudsql_to_bq_refresh_state"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/${var.project_id}/topics/v1.trigger_post_deploy_cloudsql_to_bq_refresh_state"
  }

  entry_point = "trigger_post_deploy_cloudsql_to_bq_refresh"
  environment_variables = {
    "GCP_PROJECT" = var.project_id
    "SCHEMA"      = "state"
  }

  source_repository {
    url = local.repo_url
  }
}
