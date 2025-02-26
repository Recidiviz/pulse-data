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

# Link static_reference_tables as a module
module "static_reference_tables" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "static_reference_tables"
  description = "This dataset contains (static) reference tables."
}

# Actually create the scheduled queries
# `experiments` is a log of tracked experiments (e.g. product rollouts) with 
# details about each. The source data is located in a Google Sheet.
resource "google_bigquery_data_transfer_config" "experiments" {
  display_name           = "experiments"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.static_reference_tables.dataset_id
  params = {
    destination_table_name_template = "experiments_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT *
FROM `${var.project_id}.static_reference_tables.experiments`
WHERE experiment_id IS NOT NULL
EOT
  }
}

# `experiment_assignments` is a log of which units are assigned to which experiments.
# The source data is located in a Google Sheet.
resource "google_bigquery_data_transfer_config" "experiment_assignments" {
  display_name           = "experiment_assignments"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.static_reference_tables.dataset_id
  params = {
    destination_table_name_template = "experiment_assignments_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT * 
FROM `${var.project_id}.static_reference_tables.experiment_assignments`
WHERE state_code IS NOT NULL
EOT
  }
}

# `synthetic_state_weights` weights for non-partner states used to generate a
# (synthetic) control group whose metrics approximate the specified partner state.
# The source data is located in a Google Sheet.
resource "google_bigquery_data_transfer_config" "synthetic_state_weights" {
  display_name           = "synthetic_state_weights"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.static_reference_tables.dataset_id
  params = {
    destination_table_name_template = "synthetic_state_weights_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT
  state_code,
  assignment_date,
  ARRAY(
    SELECT trim(val)
    FROM UNNEST(SPLIT(TRIM(control_states, "[]"))) val
  ) AS control_states,
  ARRAY(
    SELECT trim(val)
    FROM UNNEST(SPLIT(TRIM(control_weights, "[]"))) val
  ) AS control_weights,
  metric_matched_on,
FROM
  `${var.project_id}.static_reference_tables.synthetic_state_weights`
WHERE
  state_code IS NOT NULL
EOT
  }
}
