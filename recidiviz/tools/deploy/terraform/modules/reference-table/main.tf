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

resource "google_storage_bucket_object" "table_data" {
  name   = "${var.table_name}.csv"
  bucket = var.bucket_name
  source = "${var.recidiviz_root}/datasets/static_data/${var.table_name}.csv"
}

resource "google_bigquery_table" "table" {
  dataset_id          = var.dataset_id
  table_id            = var.table_name
  deletion_protection = false

  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = false
    max_bad_records       = 0
    source_format         = "CSV"
    csv_options {
      quote             = "\""
      skip_leading_rows = 1
    }
    source_uris = [
      "gs://${var.bucket_name}/${google_storage_bucket_object.table_data.output_name}"
    ]
  }

  schema = var.schema
}
