# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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

resource "google_logging_metric" "bq_deployed_view_too_expensive" {
  project     = var.project_id
  name        = "bq_deployed_view_too_expensive"
  description = "Count of deployed BigQuery views that exceeded their allowed materialization time, labeled by view address."

  filter = <<-EOT
    resource.type="k8s_container"
    resource.labels.container_name="base"
    (textPayload=~"BigQueryViewDagWalker Node Failure" OR jsonPayload.message=~"BigQueryViewDagWalker Node Failure")
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"

    labels {
      key         = "dataset_id"
      value_type  = "STRING"
      description = "Dataset of the view that exceeded its allowed materialization time."
    }
    labels {
      key         = "table_id"
      value_type  = "STRING"
      description = "Table/view id of the view that exceeded its allowed materialization time."
    }
  }

  label_extractors = {
    dataset_id = "REGEXP_EXTRACT(textPayload, \"dataset_id='([^']+)'\")"
    table_id   = "REGEXP_EXTRACT(textPayload, \"table_id='([^']+)'\")"
  }
}

# Counts Cloud Audit Log entries landing in the org-wide audit log bucket. The AU-5
# alert in monitoring_alert_policy.tf fires when this metric stops receiving data.
resource "google_logging_metric" "org_audit_log_bucket_entries" {
  project     = var.project_id
  name        = "org_audit_log_bucket_entries"
  description = "Count of Cloud Audit Log entries ingested into the recidiviz_org_all_audit_logs bucket."

  bucket_name = "projects/${var.project_id}/locations/global/buckets/recidiviz_org_all_audit_logs"
  filter      = "logName:\"cloudaudit.googleapis.com\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}
