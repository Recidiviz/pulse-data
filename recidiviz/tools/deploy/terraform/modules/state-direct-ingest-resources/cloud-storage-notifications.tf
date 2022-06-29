# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

module "primary_ingest" {
  source = "../cloud-storage-notification"

  bucket_name           = google_storage_bucket.direct-ingest-bucket.name
  push_endpoint         = "${var.storage_notification_endpoint_base_url}/direct/${local.is_ingest_launched ? "handle_direct_ingest_file?start_ingest=true" : "normalize_raw_file_path"}"
  service_account_email = var.storage_notification_service_account_email
  # https://cloud.google.com/pubsub/docs/push#configure_for_push_authentication
  oidc_audience = var.storage_notification_oidc_audience
}

module "secondary_ingest" {
  source = "../cloud-storage-notification"

  bucket_name           = module.secondary-direct-ingest-bucket.name
  push_endpoint         = "${var.storage_notification_endpoint_base_url}/direct/${local.is_ingest_launched ? "handle_direct_ingest_file?start_ingest=true" : "normalize_raw_file_path"}"
  service_account_email = var.storage_notification_service_account_email
  # https://cloud.google.com/pubsub/docs/push#configure_for_push_authentication
  oidc_audience = var.storage_notification_oidc_audience
}
