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

resource "google_service_account" "service-account" {
  account_id   = local.direct_ingest_formatted_str
  display_name = "A service account for ${var.state_code} Direct Ingest."
}


resource "google_storage_bucket_iam_member" "direct-ingest-buckets-member" {
  bucket = google_storage_bucket.direct-ingest-bucket.name
  role   = var.state_admin_role
  member = "serviceAccount:${google_service_account.service-account.email}"
}

resource "google_storage_bucket_iam_member" "secondary-ingest-buckets-member" {
  bucket = module.secondary-direct-ingest-bucket.name
  role   = var.state_admin_role
  member = "serviceAccount:${google_service_account.service-account.email}"
}

resource "google_storage_bucket_iam_member" "prod-only-testing-direct-ingest-buckets-member" {
  count  = var.is_production ? 1 : 0
  bucket = google_storage_bucket.prod-only-testing-direct-ingest-bucket[count.index].name
  role   = var.state_admin_role
  member = "serviceAccount:${google_service_account.service-account.email}"
}

resource "google_service_account" "dataflow_service_account" {
  account_id   = "${local.direct_ingest_formatted_str}-df"
  display_name = "A service account for ${var.state_code} Direct Ingest in Dataflow."
}

resource "google_project_iam_member" "bigquery_read_write_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.dataflow_service_account.email}"
}

resource "google_project_iam_member" "dataflow_service_agent" {
  project = var.project_id
  role    = "roles/dataflow.serviceAgent"
  member  = "serviceAccount:${google_service_account.dataflow_service_account.email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_service_account.email}"
}

resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_service_account.email}"
}

resource "google_project_iam_member" "viewer" {
  project = var.project_id
  role    = "roles/viewer"
  member  = "serviceAccount:${google_service_account.dataflow_service_account.email}"
}

resource "google_project_iam_member" "cloudprofiler_agent" {
  project = var.project_id
  role    = "roles/cloudprofiler.agent"
  member  = "serviceAccount:${google_service_account.dataflow_service_account.email}"
}

resource "google_service_account" "cf_account" {
  account_id   = "${local.direct_ingest_formatted_str}-cf"
  display_name = "A service account for ${var.state_code} cloud functions"
}
