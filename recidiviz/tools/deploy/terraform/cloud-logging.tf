// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================

# On-Call Error Logs

locals {
  on_call_dataset_id   = "on_call_logs"
  user_mgmt_dataset_id = "user_mgmt_logs"
}

resource "google_logging_project_sink" "oncall-logs-sink" {
  name        = "on-call-logs"
  description = "Sink to send app logs to BigQuery"

  # Can export to pubsub, cloud storage, bigquery, log bucket, or another project
  destination = format("bigquery.googleapis.com/projects/%s/datasets/%s", var.project_id, local.on_call_dataset_id)

  # Include HTTP request logs and app stdout logs
  filter = <<EOT
    log_id("appengine.googleapis.com/nginx.request")
    OR (log_id("app") AND severity >= "WARNING")
    OR log_id("run.googleapis.com/requests")
    OR (log_id("python") AND severity >= "WARNING")
  EOT

  # Use a unique writer (creates a unique service account used for writing)
  unique_writer_identity = true

  bigquery_options {
    use_partitioned_tables = true
  }
}

resource "google_logging_project_sink" "user-mgmt-logs-sink" {
  name        = "user-mgmt-logs"
  description = "Sink to send user management logs to BigQuery"

  # Can export to pubsub, cloud storage, bigquery, log bucket, or another project
  destination = format("bigquery.googleapis.com/projects/%s/datasets/%s", var.project_id, module.terraform_managed_bigquery_dataset[local.user_mgmt_dataset_id].dataset_id)

  # Include reasons logs from the user permissions admin panel page
  filter = <<EOT
    resource.type="cloud_run_revision"
    resource.labels.service_name="admin-panel"
    "State User Permissions"
    severity>=INFO
  EOT

  # Use a unique writer (creates a unique service account used for writing)
  unique_writer_identity = true

  bigquery_options {
    use_partitioned_tables = true
  }
}

resource "google_bigquery_dataset" "oncall_logs_dataset" {
  depends_on = [google_logging_project_sink.oncall-logs-sink]
  dataset_id = local.on_call_dataset_id
  labels     = {}

  access {
    role = "OWNER"
    user_by_email = trimprefix(
      google_logging_project_sink.oncall-logs-sink.writer_identity,
      "serviceAccount:"
    )
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  lifecycle {
    # The `protection` Resource Manager tag is stamped on this dataset out-of-band by
    # the calc DAG's ApplyDatasetProtectionTagsEntrypoint (it gates the catastrophic-
    # delete deny policy; see recidiviz/tools/deploy/deletion_protection/). Without
    # this, Terraform would try to delete the tag on every apply and 403 -- the deploy
    # SA has no resourcemanager.tagValueBindings.delete on the tag value. The reconcile
    # job is the sole owner of resource_tags. Same rationale as the shared
    # big_query_dataset module; on_call_logs is a standalone dataset, not that module.
    ignore_changes = [resource_tags]
  }
}

resource "google_bigquery_dataset_access" "user_mgmt_logs_dataset_log_writer_access" {
  dataset_id = module.terraform_managed_bigquery_dataset[local.user_mgmt_dataset_id].dataset_id
  role       = "OWNER"
  user_by_email = trimprefix(
    google_logging_project_sink.user-mgmt-logs-sink.writer_identity,
    "serviceAccount:"
  )
}

# Routes the project's default log stream to a regional, CMEK-encrypted bucket.
# The built-in _Default bucket is global and cannot move or take a key, which the
# CJIS Assured Workloads folder rejects on both counts (Recidiviz/zenhub-tasks#2609).
# Keyed by project id so every block below, including the imports, is empty in
# production until it migrates (Recidiviz/zenhub-tasks#2335).
locals {
  cmek_logging_projects = toset(var.project_id == "recidiviz-staging" ? [var.project_id] : [])
  cmek_project_id       = "cmek-82ade411-5705-4461-b2cb-9"
}

data "google_kms_key_ring" "logging_cmek" {
  for_each = local.cmek_logging_projects
  project  = local.cmek_project_id
  name     = "logging-cmek"
  location = var.us_central_region
}

resource "google_kms_crypto_key" "logging_cmek" {
  for_each        = local.cmek_logging_projects
  name            = "${each.key}-logging"
  key_ring        = data.google_kms_key_ring.logging_cmek[each.key].id
  rotation_period = "7776000s" # 90 days

  lifecycle {
    prevent_destroy = true
  }
}

# The key already exists in the CMEK project; the registry adopts it. This block
# can be removed once staging has applied it.
import {
  for_each = local.cmek_logging_projects
  to       = google_kms_crypto_key.logging_cmek[each.key]
  id       = "projects/${local.cmek_project_id}/locations/${var.us_central_region}/keyRings/logging-cmek/cryptoKeys/${each.key}-logging"
}

# Cloud Logging writes to the bucket as the project's Logging service agent.
resource "google_kms_crypto_key_iam_member" "logging_sa_cmek_user" {
  for_each      = local.cmek_logging_projects
  crypto_key_id = google_kms_crypto_key.logging_cmek[each.key].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-logging.iam.gserviceaccount.com"
}

resource "google_logging_project_bucket_config" "default_regional" {
  for_each       = local.cmek_logging_projects
  project        = each.key
  location       = var.us_central_region
  bucket_id      = "default-regional"
  retention_days = 30
  description    = "Regional, CMEK-encrypted destination for the _Default sink"
  # The global _Default bucket has Log Analytics on; it cannot be turned off once on.
  enable_analytics = true

  cmek_settings {
    kms_key_name = google_kms_crypto_key.logging_cmek[each.key].id
  }

  depends_on = [google_kms_crypto_key_iam_member.logging_sa_cmek_user]

  lifecycle {
    prevent_destroy = true
  }
}

# The project's built-in _Default sink, adopted here so its destination can be
# pointed at the regional bucket. The filter is the one Cloud Logging creates.
resource "google_logging_project_sink" "default" {
  for_each    = local.cmek_logging_projects
  name        = "_Default"
  destination = "logging.googleapis.com/${google_logging_project_bucket_config.default_regional[each.key].id}"
  # Matches the single-line filter Cloud Logging stores, so imports plan no filter change.
  filter = "NOT LOG_ID(\"cloudaudit.googleapis.com/activity\") AND NOT LOG_ID(\"externalaudit.googleapis.com/activity\") AND NOT LOG_ID(\"cloudaudit.googleapis.com/system_event\") AND NOT LOG_ID(\"externalaudit.googleapis.com/system_event\") AND NOT LOG_ID(\"cloudaudit.googleapis.com/access_transparency\") AND NOT LOG_ID(\"externalaudit.googleapis.com/access_transparency\")"

  unique_writer_identity = true
}

# This block can be removed once staging has applied it.
import {
  for_each = local.cmek_logging_projects
  to       = google_logging_project_sink.default[each.key]
  id       = "projects/${each.key}/sinks/_Default"
}
