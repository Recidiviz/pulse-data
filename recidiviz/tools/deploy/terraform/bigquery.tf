# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

resource "google_project_service" "bigquery_connection_api" {
  service = "bigqueryconnection.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

# Storing validation results
module "validation_results_dataset" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "validation_results"
  description = "This dataset contains raw results from data validation runs as well as any views over them."
}

resource "google_bigquery_table" "validation_results" {
  dataset_id          = module.validation_results_dataset.dataset_id
  table_id            = "validation_results"
  description         = "This table contains the results from data validation runs."
  deletion_protection = false

  schema = <<EOF
[
    {
        "name": "run_id",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "run_date",
        "type": "DATE",
        "mode": "REQUIRED"
    },
    {
        "name": "system_version",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "check_type",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "validation_name",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "region_code",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "did_run",
        "type": "BOOLEAN",
        "mode": "REQUIRED"
    },
    {
        "name": "run_datetime",
        "type": "DATETIME",
        "mode": "NULLABLE"
    },
    {
        "name": "validation_category",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "was_successful",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },
    {
        "name": "validation_result_status",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "result_details_type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "result_details",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "failure_description",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF

}

module "supplemental_generated_dataset" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "supplemental_data"
  description = "This dataset contains tables with generated data that does not go through the traditional ingest and calc pipelines."
}

module "export_archives_dataset" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "export_archives"
  description = "This dataset contains tables that archive the contents of daily exports."
}

resource "google_bigquery_table" "workflows_client_record_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "workflows_client_record_archive"
  description         = "This table contains daily archives of the client_record export for Workflows, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "NEWLINE_DELIMITED_JSON"
    # while archives do also exist in production, we have been using staging exports
    # as the sole data source for Workflows in production; for consistency we are doing
    # the same with this table
    source_uris = ["gs://recidiviz-staging-practices-etl-data-archive/*/client_record.json"]
  }

  schema = <<EOF
[
    {
        "name": "person_external_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "pseudonymized_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "district",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "compliant_reporting_eligible",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "officer_id",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF

}
