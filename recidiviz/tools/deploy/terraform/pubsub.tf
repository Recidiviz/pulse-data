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

resource "google_pubsub_topic" "raw_data_storage_notification_topic" {
  name = "v1.ingest.raw_data_storage_notifications"
}

resource "google_pubsub_topic" "utah_data_transfer_sync_notifications" {
  name = "v1.ingest.utah_data_transfer_sync_notifications"
}

data "google_storage_project_service_account" "gcs_account" {
}

// Enable Cloud Storage notifications to this topic by giving the correct IAM permission to the GCS-specific service
// account that will publish Cloud Storage notifications.
resource "google_pubsub_topic_iam_binding" "binding" {
  topic   = google_pubsub_topic.raw_data_storage_notification_topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}
