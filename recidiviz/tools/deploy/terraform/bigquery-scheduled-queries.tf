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

# This sets up scheduled queries to materialize tables on a regular schedule, e.g.
# those that are sourced from Google Sheets.

# Enable the data transfer service
resource "google_project_service" "data_transfer_service" {
  project                    = var.project_id
  service                    = "bigquerydatatransfer.googleapis.com"
  disable_dependent_services = false
  disable_on_destroy         = false
}

# Create a service account with the proper permissions
resource "google_service_account" "bigquery_scheduled_queries" {
  account_id   = "bigquery-scheduled-queries"
  display_name = "BigQuery Scheduled Queries Service Account"
  description  = "Account that runs BigQuery scheduled queries."
}

resource "google_project_iam_member" "bigquery_datatransfer_admin" {
  project = var.project_id
  # See following link for why this role is required:
  # https://cloud.google.com/bigquery/docs/use-service-accounts#required_permissions 
  role   = "roles/bigquery.admin"
  member = "serviceAccount:${google_service_account.bigquery_scheduled_queries.email}"
}

resource "google_bigquery_data_transfer_config" "product_roster_archive" {
  display_name           = "product_roster_archive"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 03:00" # In UTC, gives us end of day in US
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.terraform_managed_bigquery_dataset["export_archives"].dataset_id

  params = {
    destination_table_name_template = "product_roster_archive"
    write_disposition               = "WRITE_APPEND"
    query                           = <<-EOT
SELECT CURRENT_DATE("US/Eastern") AS export_date, *
FROM `${var.project_id}.reference_views.product_roster`
EOT
  }
}

resource "google_bigquery_data_transfer_config" "normalized_state_hydration_archive" {
  display_name           = "normalized_state_hydration_archive"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 03:00" # In UTC, gives us end of day in US
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = "hydration_archive"

  params = {
    destination_table_name_template = "normalized_state_hydration_archive"
    write_disposition               = "WRITE_APPEND"
    query                           = <<-EOT
SELECT CURRENT_DATE("US/Eastern") AS hydration_date, *
FROM `${var.project_id}.platform_kpis.normalized_state_hydration_live_snapshot`
EOT
  }
}
