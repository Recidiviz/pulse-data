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

# Logs-based metrics that surface the Kubernetes scheduler events behind the
# `raw_data_chunk_normalization` pod-preemption flakiness (OBT-2245). GKE writes
# pod lifecycle events to the `events` log; we count the two that matter for
# right-sizing and warm-pool work and feed them into the dashboard's Row 3.

# Counter of Preempted events for raw-data import pods. A spike here is the
# chunk-normalization preemption we are trying to eliminate.
resource "google_logging_metric" "raw_data_pod_preemptions" {
  project     = var.project_id
  name        = "raw_data_pod_preemptions"
  description = "Count of Kubernetes Preempted events for raw-data import pods (raw-data-file-chunking / raw-data-chunk-normalization). Used by the Raw Data Import monitoring dashboard."

  filter = <<-EOT
    log_id("events")
    jsonPayload.reason="Preempted"
    jsonPayload.involvedObject.name:"raw-data-"
    NOT jsonPayload.involvedObject.name=~"^warm-pool-"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"

    labels {
      key         = "step"
      value_type  = "STRING"
      description = "Raw-data import step the preempted pod belongs to (file-chunking or chunk-normalization)."
    }
  }

  label_extractors = {
    # raw-data-chunk-normalization-<suffix> -> chunk-normalization
    "step" = "REGEXP_EXTRACT(jsonPayload.involvedObject.name, \"raw-data-(file-chunking|chunk-normalization)\")"
  }
}

# Counter of FailedScheduling events for raw-data import pods. These fire during
# the node-provisioning scramble (e.g. "Insufficient cpu") that precedes the
# preemptions, and motivate the warm capacity pool (OBT-35079).
resource "google_logging_metric" "raw_data_pod_failed_scheduling" {
  project     = var.project_id
  name        = "raw_data_pod_failed_scheduling"
  description = "Count of Kubernetes FailedScheduling events for raw-data import pods. Used by the Raw Data Import monitoring dashboard."

  filter = <<-EOT
    log_id("events")
    jsonPayload.reason="FailedScheduling"
    jsonPayload.involvedObject.name:"raw-data-"
    NOT jsonPayload.involvedObject.name=~"^warm-pool-"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"

    labels {
      key         = "step"
      value_type  = "STRING"
      description = "Raw-data import step the unschedulable pod belongs to (file-chunking or chunk-normalization)."
    }
  }

  label_extractors = {
    "step" = "REGEXP_EXTRACT(jsonPayload.involvedObject.name, \"raw-data-(file-chunking|chunk-normalization)\")"
  }
}
