# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

module "direct-ingest-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = local.direct_ingest_formatted_str
  location    = var.region
  use_cmek    = var.use_cmek
}

# Tells Terraform this bucket was previously a raw google_storage_bucket
# and is now managed by the cloud-storage-bucket module. Without this,
# Terraform would delete the old bucket and create a new one (data loss).
moved {
  from = google_storage_bucket.direct-ingest-bucket
  to   = module.direct-ingest-bucket.google_storage_bucket.bucket
}

# expose the primary ingest bucket name
output "primary_ingest_bucket_name" {
  value = module.direct-ingest-bucket.name
}

resource "google_storage_notification" "direct-ingest-bucket-notification" {
  bucket         = module.direct-ingest-bucket.name
  topic          = var.raw_data_storage_notification_topic_id
  event_types    = ["OBJECT_FINALIZE"]
  payload_format = "JSON_API_V1"
}

module "prod-only-testing-direct-ingest-bucket" {
  count  = var.is_production ? 1 : 0
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "${local.direct_ingest_formatted_str}-upload-testing"
  location    = var.region
  use_cmek    = var.use_cmek
}

moved {
  from = google_storage_bucket.prod-only-testing-direct-ingest-bucket
  to   = module.prod-only-testing-direct-ingest-bucket.google_storage_bucket.bucket
}

module "secondary-direct-ingest-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "${local.direct_ingest_formatted_str}-secondary"
  location    = var.region
  use_cmek    = var.use_cmek
}

resource "google_storage_notification" "secondary-direct-ingest-bucket-notification" {
  bucket         = module.secondary-direct-ingest-bucket.name
  topic          = var.raw_data_storage_notification_topic_id
  event_types    = ["OBJECT_FINALIZE"]
  payload_format = "JSON_API_V1"
}

# Bucket used to store any supplemental data provided by the state that is not run
# through direct ingest, e.g. validation data.
module "supplemental-data-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "${local.direct_ingest_formatted_str}-supplemental"
  location    = var.region
  use_cmek    = var.use_cmek
}
