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
  on_call_dataset_id = "on_call_logs"
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

resource "google_bigquery_dataset" "oncall_logs_dataset" {
  depends_on = [google_logging_project_sink.oncall-logs-sink]
  dataset_id = local.on_call_dataset_id
  labels     = {}

  access {
    role          = "OWNER"
    user_by_email = google_logging_project_sink.oncall-logs-sink.writer_identity
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
}
