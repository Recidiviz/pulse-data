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

locals {
  repo_url = "https://source.developers.google.com/projects/${var.project_id}/repos/github_Recidiviz_pulse-data/revisions/${var.git_hash}/paths/recidiviz/cloud_functions"
}

# Cloud Functions that trigger file name normalization and nothing else for buckets designated as automatic upload
# test beds.
resource "google_cloudfunctions_function" "direct-ingest-states-upload-testing" {
  # Buckets ending in `upload-testing` are only present in prod.
  for_each = local.is_production ? toset(["US_MO", "US_TN", "US_MI"]) : toset([])

  name    = "direct-ingest-state-${replace(lower(each.key), "_", "-")}-upload-testing"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = "${var.project_id}-direct-ingest-state-${replace(lower(each.key), "_", "-")}-upload-testing"
  }

  entry_point = "normalize_raw_file_path"
  environment_variables = {
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }
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


resource "google_cloudfunctions_function" "parse-state-aggregate" {
  name    = "parse-state-aggregate"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  available_memory_mb = 2048

  entry_point = "parse_state_aggregate"
  environment_variables = {
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }

  timeout = 540
}


resource "google_cloudfunctions_function" "report_deliver_emails_for_batch" {
  name    = "report_deliver_emails_for_batch"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point = "handle_deliver_emails_for_batch_email_reporting"
  environment_variables = {
    "FROM_EMAIL_ADDRESS" = "reports@recidiviz.org"
    "FROM_EMAIL_NAME"    = "Recidiviz Reports"
    "GCP_PROJECT"        = var.project_id
    "SENDGRID_API_KEY"   = data.google_secret_manager_secret_version.sendgrid_api_key.secret_data
  }
  trigger_http = true

  source_repository {
    url = local.repo_url
  }

  timeout = 300

  timeouts {}
}


resource "google_cloudfunctions_function" "report_start_new_batch" {
  name    = "report_start_new_batch"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point = "handle_start_new_batch_email_reporting"
  environment_variables = {
    "CDN_STATIC_IP" = data.google_secret_manager_secret_version.po_report_cdn_static_ip.secret_data
    "GCP_PROJECT"   = var.project_id
  }
  trigger_http = true

  source_repository {
    url = local.repo_url
  }

  timeout = 300
}

resource "google_cloudfunctions_function" "trigger_daily_calculation_pipeline_dag" {
  name    = "trigger_daily_calculation_pipeline_dag"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/${var.project_id}/topics/v1.calculator.trigger_daily_pipelines"
  }

  entry_point = "trigger_daily_calculation_pipeline_dag"
  environment_variables = {
    # This is an output variable from the composer environment, relevant docs:
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
    "AIRFLOW_URI" = google_composer_environment.default.config.0.airflow_uri
    "GCP_PROJECT" = var.project_id
    # Gets the IAP client id to use when talking to airflow from our custom python source.
    "IAP_CLIENT_ID" = data.external.composer_iap_client_id.result.iap_client_id
  }

  source_repository {
    url = local.repo_url
  }
}

# Cloud Function that calls an endpoint to update Auth0 users with the updated user restrictions file from the
# triggering bucket
resource "google_cloudfunctions_function" "handle_state_dashboard_user_restrictions_file" {
  name    = "handle_state_dashboard_user_restrictions_file"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = "${var.project_id}-dashboard-user-restrictions"
  }

  entry_point           = "handle_state_dashboard_user_restrictions_file"
  environment_variables = {}

  source_repository {
    url = local.repo_url
  }
}
