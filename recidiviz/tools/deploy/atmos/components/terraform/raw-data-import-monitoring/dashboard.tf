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

# Cloud Monitoring dashboard showing raw-data import pod CPU/memory usage vs.
# requests and the recidiviz-pod-node (Autopilot NAP) cluster capacity, so we
# can right-size pod resources (OBT-35078), watch the warm pool (OBT-35079), and
# catch raw_data_chunk_normalization preemptions (OBT-2245) at a glance.
#
# The Row 3 tiles read the logs-based metrics defined in logging_metrics.tf, so
# the dashboard depends on them implicitly via the metric type strings.
resource "google_monitoring_dashboard" "raw_data_import" {
  project = var.project_id

  dashboard_json = templatefile(
    "${path.module}/dashboard.json.tftpl",
    {
      project_id = var.project_id
      # Referencing the .name attributes (rather than hardcoding the strings)
      # makes the dashboard depend on the metrics, so they are created first.
      preemptions_metric_type       = "logging.googleapis.com/user/${google_logging_metric.raw_data_pod_preemptions.name}"
      failed_scheduling_metric_type = "logging.googleapis.com/user/${google_logging_metric.raw_data_pod_failed_scheduling.name}"
    }
  )
}
