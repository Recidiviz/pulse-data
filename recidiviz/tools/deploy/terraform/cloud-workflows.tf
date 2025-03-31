# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
  cloud_workflows_repo_path = "${local.recidiviz_root}/cloud_workflows/"
  cloud_workflows_sa_roles = [
    "roles/pubsub.publisher",       # for event arc trigger
    "roles/eventarc.eventReceiver", # for event arc trigger
    "roles/logging.logWriter",      # for writing to logs while in a workflow
    "roles/workflows.invoker",      # for running workflows
    "roles/run.admin",              # for executing cloud run jobs from a workflows
  ]
}

resource "google_service_account" "cloud_workflows" {
  account_id   = "cloud-workflows-sa"
  display_name = "Cloud Workflows Service Account"
  description  = <<EOT
Service Account that acts as the identity for Cloud Workflows service. The account and 
its IAM policies are managed in Terraform (see #39988).
EOT 
}

resource "google_project_iam_member" "cloud_workflows_iam" {
  for_each = toset(local.cloud_workflows_sa_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.cloud_workflows.email}"
}


resource "google_workflows_workflow" "utah_data_transfer_sync_cloud_run_connector" {
  name            = "utah-data-transfer-sync-cloud-run-connector"
  description     = "Workflow that parses the BigQuery transfer completion event of the `ut-udc-dw-dev-to-recidiviz-ingest-us-ut-daily-sync` big query transfer running in `recidiviz-ingest-us-ut` and triggers a cloud run job that will export all tables that have a file tag managed in code to Utah's ingest bucket."
  region          = var.us_central_region
  service_account = google_service_account.cloud_workflows.id
  call_log_level  = "LOG_ALL_CALLS"
  user_env_vars = {
    CLOUD_RUN_JOB_NAME     = google_cloud_run_v2_job.utah_data_transfer_sync.name
    CLOUD_RUN_JOB_LOCATION = var.us_central_region

  }
  source_contents = file("${local.cloud_workflows_repo_path}/utah_data_transfer_sync_cloud_run_connector.yaml")
}


resource "google_eventarc_trigger" "utah_data_transfer_sync_cloud_run_connector_trigger" {
  name            = "utah-data-transfer-sync-cloud-run-connector-trigger"
  location        = var.us_central_region
  service_account = google_service_account.cloud_workflows.id

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.utah_data_transfer_sync_notifications.id
    }
  }

  destination {
    workflow = google_workflows_workflow.utah_data_transfer_sync_cloud_run_connector.id
  }
}
