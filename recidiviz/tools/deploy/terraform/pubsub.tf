# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

resource "google_pubsub_topic" "airflow_monitoring_topic" {
  name = "v1.airflow_monitoring.trigger_hourly_monitoring_dag"
}

resource "google_pubsub_topic" "sftp_pubsub_topic" {
  name = "v1.sftp.trigger_sftp_dag"
}

resource "google_pubsub_topic" "raw_data_import_dag_pubsub_topic" {
  name = "v1.ingest.trigger_raw_data_import_dag"
}

resource "google_pubsub_topic" "raw_data_storage_notification_topic" {
  name = "v1.ingest.raw_data_storage_notifications"
}
