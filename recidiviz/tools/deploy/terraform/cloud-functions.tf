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

# The execute-covid-aggregation is not included because it will eventually
# be deprecated. See #4464.

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
  repo_url      = "https://source.developers.google.com/projects/${var.project_id}/repos/github_Recidiviz_pulse-data/revisions/${var.git_hash}/paths/recidiviz/cloud_functions"
  is_production = (var.project_id == "recidiviz-123")
}


resource "google_cloudfunctions_function" "direct-ingest-states" {
  # TODO(#4690): The cloud function for US_MO ingest on prod apparently oscillates
  # between a "handle_state_direct_ingest_file" and
  # "handle_state_direct_ingest_file_rename_only" entrypoint. As a result, prod
  # and staging drift :(. Once that's more stable, we can add it to this list.
  for_each = toset(["US_ID", "US_ND", "US_PA"])

  name    = "direct-ingest-state-${replace(lower(each.key), "_", "-")}"
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point           = "handle_state_direct_ingest_file"
  environment_variables = {}

  source_repository {
    url = local.repo_url
  }

  timeouts {}
}


resource "google_cloudfunctions_function" "export_metric_view_data" {
  name    = "export_metric_view_data"
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point           = "export_metric_view_data"
  environment_variables = {}

  source_repository {
    url = local.repo_url
  }

  # TODO(#4379): This should be removed when we have a more scalable solution
  # for preventing 499s in the metric export pipeline. Longer term, we want this
  # cloud function to issue an async request and return an id that can be queried
  # by another process.
  timeout = 540

  timeouts {}
}


resource "google_cloudfunctions_function" "handle-covid-source-upload" {
  name    = "handle-covid-source-upload"
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  available_memory_mb = 2048

  entry_point           = "handle_covid_ingest_on_upload"
  environment_variables = {}

  source_repository {
    url = local.repo_url
  }

  timeout = 500

  timeouts {}
}

resource "google_cloudfunctions_function" "parse-state-aggregate" {
  name    = "parse-state-aggregate"
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  available_memory_mb = 2048

  entry_point           = "parse_state_aggregate"
  environment_variables = {}

  source_repository {
    url = local.repo_url
  }

  timeout = 540

  timeouts {}
}


resource "google_cloudfunctions_function" "report_deliver_emails_for_batch" {
  name    = "report_deliver_emails_for_batch"
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point = "handle_deliver_emails_for_batch_email_reporting"
  environment_variables = {
    "FROM_EMAIL_ADDRESS" = "reports@recidiviz.org"
    "FROM_EMAIL_NAME"    = "Recidiviz Reports"
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
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point = "handle_start_new_batch_email_reporting"
  environment_variables = {
    "CDN_STATIC_IP" = data.google_secret_manager_secret_version.po_report_cdn_static_ip.secret_data
  }
  trigger_http = true

  source_repository {
    url = local.repo_url
  }

  timeout = 300

  timeouts {}
}

resource "google_cloudfunctions_function" "run_calculation_pipelines" {
  name    = "run_calculation_pipelines"
  runtime = "python37"
  labels = {
    "deployment-tool" = "terraform"
  }

  entry_point = "trigger_calculation_pipeline_dag"
  environment_variables = {
    "WEBSERVER_ID" = local.is_production ? "p03ca791d5f21b85cp-tp" : "jef8828f38bc9738ap-tp"
  }

  source_repository {
    url = local.repo_url
  }

  timeouts {}
}
