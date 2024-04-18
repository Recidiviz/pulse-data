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

locals {
  gcs_file_name = "${var.table_name}.csv"
  source_tables = "${var.recidiviz_root}/big_query/source_tables/schema"
}

resource "google_storage_bucket_object" "table_data" {
  # If this is a code file-backed reference table, we must first upload
  # the local file to GCS.
  count  = var.read_from_local ? 1 : 0
  name   = local.gcs_file_name
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
      "gs://${var.bucket_name}/${local.gcs_file_name}"
    ]
  }

  schema = jsonencode(yamldecode(file("${local.source_tables}/${var.dataset_id}/${var.table_name}.yaml"))["schema"])
}
