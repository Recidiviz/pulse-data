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

# Link experiments_metadata as a module
module "experiments_metadata" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "experiments_metadata"
  description = "This dataset contains the metadata for our experiments."
}

# Link static_reference_tables as a module
module "static_reference_tables" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "static_reference_tables"
  description = "This dataset contains (static) reference tables."
}

# Link google_sheet_backed_tables as a module
module "google_sheet_backed_tables" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "google_sheet_backed_tables"
  description = "This dataset contains tables backed by Google Sheets."
}

# Link manually_updated_source_tables as a module
module "manually_updated_source_tables" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "manually_updated_source_tables"
  description = "This dataset includes source tables that are updated manually at some cadence, e.g. via a script or a manual BQ query in the UI to insert rows. Descriptions for tables added to this dataset should include information about how/when the table is updated."
}

# `workflows_launch_metadata` contains information about each fully launched Workflows opportunity in our states.
# The source data is located in a Google Sheet.
resource "google_bigquery_data_transfer_config" "workflows_launch_metadata" {
  display_name           = "workflows_launch_metadata"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.static_reference_tables.dataset_id
  params = {
    destination_table_name_template = "workflows_launch_metadata_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT * 
FROM `${var.project_id}.static_reference_tables.workflows_launch_metadata`
EOT
  }
}

resource "google_bigquery_data_transfer_config" "product_roster_archive" {
  display_name           = "product_roster_archive"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 03:00" # In UTC, gives us end of day in US
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.export_archives_dataset.dataset_id

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



resource "google_monitoring_alert_policy" "scheduled_query_monitoring" {
  display_name          = "BQ Scheduled Query Monitoring"
  notification_channels = [data.google_monitoring_notification_channel.infra_pagerduty.name]
  combiner              = "OR"
  conditions {
    display_name = "Log match condition: failed scheduled query"
    condition_matched_log {
      filter = "resource.type=\"bigquery_dts_config\" severity=\"ERROR\""
    }
  }

  alert_strategy {
    notification_rate_limit {
      period = "900s"
    }
  }

  documentation {
    content = "A BQ scheduled query failed. See history at https://console.cloud.google.com/bigquery/transfers?project=${var.project_id}"
  }
}
