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
  dataset_id  = module.validation_results_dataset.dataset_id
  table_id    = "validation_results"
  description = "This table contains the results from data validation runs."

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
