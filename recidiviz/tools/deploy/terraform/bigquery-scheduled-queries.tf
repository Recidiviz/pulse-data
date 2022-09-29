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

# This sets up scheduled queries to materialize tables that are too slow to materialize
# as part of our regular flow.

# Allow us to access the project number
data "google_project" "project" {}

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

resource "google_project_iam_member" "bigquery_scheduled_queries_permissions" {
  project = var.project_id
  role    = "roles/iam.serviceAccountShortTermTokenMinter"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}
resource "google_project_iam_binding" "bigquery_datatransfer_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  members = ["serviceAccount:${google_service_account.bigquery_scheduled_queries.email}"]
}

# Create a dataset
module "unmanaged_views_dataset" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "unmanaged_views"
  description = "This dataset contains materialized versions of views that are managed by scheduled queries instead of our typical view materialization."

  depends_on = [google_project_iam_member.bigquery_scheduled_queries_permissions]
}

# Actually create the scheduled queries
resource "google_bigquery_data_transfer_config" "compartment_sessions_unnested_materialized" {
  display_name           = "compartment_sessions_unnested_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "compartment_sessions_unnested_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.sessions.compartment_sessions_unnested`"
  }

  depends_on = [google_project_iam_member.bigquery_scheduled_queries_permissions]
}


resource "google_bigquery_data_transfer_config" "supervision_district_metrics_materialized" {
  display_name           = "supervision_district_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_district_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_district_metrics`"
  }

  depends_on = [google_project_iam_member.bigquery_scheduled_queries_permissions]
}

resource "google_bigquery_data_transfer_config" "supervision_office_metrics_materialized" {
  display_name           = "supervision_office_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_office_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_office_metrics`"
  }

  depends_on = [google_project_iam_member.bigquery_scheduled_queries_permissions]
}

resource "google_bigquery_data_transfer_config" "supervision_officer_metrics_materialized" {
  display_name           = "supervision_officer_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_officer_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_officer_metrics`"
  }

  depends_on = [google_project_iam_member.bigquery_scheduled_queries_permissions]
}

resource "google_bigquery_data_transfer_config" "supervision_state_metrics_materialized" {
  display_name           = "supervision_state_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_state_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_state_metrics`"
  }

  depends_on = [google_project_iam_member.bigquery_scheduled_queries_permissions]
}
