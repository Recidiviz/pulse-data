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

locals {
  source_tables = "${local.recidiviz_root}/source_tables/schema/"
}

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

  schema = jsonencode(yamldecode(file("${local.source_tables}/validation_results/validation_results.yaml"))["schema"])
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
    source_uris           = ["gs://${var.project_id}-practices-etl-data-archive/*/client_record.json"]
  }

  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/workflows_client_record_archive.yaml"))["schema"])
}

resource "google_bigquery_table" "workflows_resident_record_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "workflows_resident_record_archive"
  description         = "This table contains daily archives of the resident_record export for Workflows, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "NEWLINE_DELIMITED_JSON"
    source_uris           = ["gs://${var.project_id}-practices-etl-data-archive/*/resident_record.json"]
  }

  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/workflows_resident_record_archive.yaml"))["schema"])
}

# This legacy table should be used to add any additional compliant reporting legacy fields from `client_record`
# that we might need for impact tracking. Historically these fields were on client_record, and they have been moved
# to compliant_reporting_referral_record.
resource "google_bigquery_table" "workflows_legacy_client_record_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "workflows_legacy_client_record_archive"
  description         = "This table contains daily archives of the client_record export for Workflows, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "NEWLINE_DELIMITED_JSON"
    source_uris           = ["gs://${var.project_id}-practices-etl-data-archive/*/client_record.json"]
  }
  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/workflows_legacy_client_record_archive.yaml"))["schema"])
}

resource "google_bigquery_table" "workflows_compliant_reporting_referral_record_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "workflows_compliant_reporting_referral_record_archive"
  description         = "This table contains daily archives of the compliant_reporting_referral_record export for Workflows, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "NEWLINE_DELIMITED_JSON"
    source_uris           = ["gs://${var.project_id}-practices-etl-data-archive/*/compliant_reporting_referral_record.json"]
  }
  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/workflows_compliant_reporting_referral_record_archive.yaml"))["schema"])
}

resource "google_bigquery_table" "outliers_supervision_officer_outlier_status_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "outliers_supervision_officer_outlier_status_archive"
  description         = "This table contains daily archives of the supervision_officer_outlier_status export for Outliers, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "CSV"
    source_uris           = ["gs://${var.project_id}-outliers-etl-data-archive/*/supervision_officer_outlier_status.csv"]
  }
  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/outliers_supervision_officer_outlier_status_archive.yaml"))["schema"])

}

resource "google_bigquery_table" "outliers_supervision_officer_supervisors_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "outliers_supervision_officer_supervisors_archive"
  description         = "This table contains daily archives of the supervision_officer_supervisors export for Outliers, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "CSV"
    source_uris           = ["gs://${var.project_id}-outliers-etl-data-archive/*/supervision_officer_supervisors.csv"]
  }
  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/outliers_supervision_officer_supervisors_archive.yaml"))["schema"])

}

resource "google_bigquery_table" "outliers_supervision_officers_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "outliers_supervision_officers_archive"
  description         = "This table contains daily archives of the supervision_officers export for Outliers, which are read directly from Cloud Storage."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "CSV"
    source_uris           = ["gs://${var.project_id}-outliers-etl-data-archive/*/supervision_officers.csv"]
  }
  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/outliers_supervision_officers_archive.yaml"))["schema"])
}

resource "google_bigquery_table" "workflows_snooze_status_archive" {
  dataset_id          = module.export_archives_dataset.dataset_id
  table_id            = "workflows_snooze_status_archive"
  description         = "This table contains daily archives of active opportunity snoozes and denials, exported from Firestore."
  deletion_protection = false
  external_data_configuration {
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 0
    source_format         = "NEWLINE_DELIMITED_JSON"
    source_uris           = ["gs://${var.project_id}-snooze-status-archive/*.json"]
  }
  schema = jsonencode(yamldecode(file("${local.source_tables}/${module.export_archives_dataset.dataset_id}/workflows_snooze_status_archive.yaml"))["schema"])
}
