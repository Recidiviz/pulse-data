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

module "external_reference_tables_bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "external-reference-data"
}

resource "google_bigquery_dataset" "external_reference" {
  dataset_id  = "external_reference"
  description = "Contains reference tables from external sources that are synced from our repository."
  location    = "US"
}

module "county_resident_adult_populations_table" {
  source = "./modules/reference-table"

  project_id  = var.project_id
  bucket_name = module.external_reference_tables_bucket.name
  dataset_id  = google_bigquery_dataset.external_reference.dataset_id

  table_name = "county_resident_adult_populations"
  schema     = <<EOF
[
  {
    "name": "fips",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "year",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "population",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
EOF
}

module "county_resident_populations_table" {
  source = "./modules/reference-table"

  project_id  = var.project_id
  bucket_name = module.external_reference_tables_bucket.name
  dataset_id  = google_bigquery_dataset.external_reference.dataset_id

  table_name = "county_resident_populations"
  schema     = <<EOF
[
  {
    "name": "fips",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "year",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "population",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
EOF
}

module "county_fips_table" {
  source = "./modules/reference-table"

  project_id  = var.project_id
  bucket_name = module.external_reference_tables_bucket.name
  dataset_id  = google_bigquery_dataset.external_reference.dataset_id

  table_name = "county_fips"
  schema     = <<EOF
[
  {
    "name": "fips",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "state_code",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "county_code",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "county_name",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}
