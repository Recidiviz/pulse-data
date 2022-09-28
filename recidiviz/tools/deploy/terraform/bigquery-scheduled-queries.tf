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

# Materializes tables that are too slow to materialize as part of our regular flow.
module "unmanaged_views_dataset" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "unmanaged_views"
  description = "This dataset contains materialized versions of views that are managed by scheduled queries instead of our typical view materialization."
}

resource "google_bigquery_data_transfer_config" "supervision_district_metrics_materialized" {
  display_name           = "supervision_district_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_district_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_district_metrics"
  }
}

resource "google_bigquery_data_transfer_config" "supervision_office_metrics_materialized" {
  display_name           = "supervision_office_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_office_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_office_metrics"
  }
}

resource "google_bigquery_data_transfer_config" "supervision_officer_metrics_materialized" {
  display_name           = "supervision_officer_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_officer_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_officer_metrics"
  }
}

resource "google_bigquery_data_transfer_config" "supervision_state_metrics_materialized" {
  display_name           = "supervision_state_metrics_materialized"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 08:00" # In UTC
  destination_dataset_id = module.unmanaged_views_dataset.dataset_id
  params = {
    destination_table_name_template = "supervision_state_metrics_materialized"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = "SELECT * FROM `${var.project_id}.analyst_data.supervision_state_metrics"
  }
}
