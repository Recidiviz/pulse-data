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

data "google_project" "staging" {
  project_id = "recidiviz-staging"
}

data "google_project" "production" {
  project_id = "recidiviz-123"
}

locals {
  staging_compute_sa    = "${data.google_project.staging.number}-compute@developer.gserviceaccount.com"
  production_compute_sa = "${data.google_project.production.number}-compute@developer.gserviceaccount.com"
}

resource "google_bigquery_dataset" "metadata" {
  dataset_id  = "metadata"
  project     = var.project_id
  location    = "US"
  description = "Dataset for metadata tables tracking various sync and export operations."
}

resource "google_bigquery_table" "bq_export_tracker" {
  dataset_id          = google_bigquery_dataset.metadata.dataset_id
  table_id            = "bq_raw_data_file_export_tracker"
  project             = var.project_id
  deletion_protection = true

  description = "Tracks exports from BigQuery tables to raw data files in the ingest bucket. Used to prevent duplicate exports of the same table update_datetime."

  schema = jsonencode([
    {
      name        = "table_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Source BQ table name"
    },
    {
      name        = "update_datetime"
      type        = "DATETIME"
      mode        = "REQUIRED"
      description = "The last modified time of the source table (UTC)"
    },
    {
      name        = "destination_project_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Project where the raw data file is being exported"
    },
    {
      name        = "destination_instance"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Raw data instance (PRIMARY/SECONDARY)"
    },
    {
      name        = "export_start_datetime"
      type        = "DATETIME"
      mode        = "REQUIRED"
      description = "When the export started (UTC)"
    },
    {
      name        = "export_status"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Current status (STARTED/COMPLETED/FAILED)"
    },
    {
      name        = "row_write_datetime"
      type        = "DATETIME"
      mode        = "REQUIRED"
      description = "When this row was written to the table (UTC)"
    }
  ])

  # Clustering optimized for the primary query pattern in sync_bq_mirror_to_ingest_bucket.py
  # which filters by destination_project_id, destination_instance, export_status, then groups by table_id
  clustering = ["destination_project_id", "destination_instance", "export_status", "table_id"]
}

resource "google_bigquery_table_iam_member" "bq_export_tracker_editor_staging" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.metadata.dataset_id
  table_id   = google_bigquery_table.bq_export_tracker.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.staging_compute_sa}"
}

resource "google_bigquery_table_iam_member" "bq_export_tracker_editor_production" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.metadata.dataset_id
  table_id   = google_bigquery_table.bq_export_tracker.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.production_compute_sa}"
}
