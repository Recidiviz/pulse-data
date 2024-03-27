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
        "name": "trace_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "exception_log",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "failure_description",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "runtime_seconds",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    },
    {
        "name": "ingest_instance",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "sandbox_dataset_prefix",
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
    source_uris           = ["gs://${var.project_id}-practices-etl-data-archive/*/client_record.json"]
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
        "name": "remaining_criteria_needed",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "officer_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "expiration_date",
        "type": "DATE",
        "mode": "NULLABLE"
    },
    {
        "name": "all_eligible_opportunities",
        "type": "STRING",
        "mode": "REPEATED"
    }
]
EOF

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
        "name": "person_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "officer_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "facility_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "all_eligible_opportunities",
        "type": "STRING",
        "mode": "REPEATED"
    }
]
EOF

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

  schema = <<EOF
[
    {
        "name": "person_external_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "compliant_reporting_eligible",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "remaining_criteria_needed",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_time_on_supervision_level",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_drug_screen",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_fines_fees",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_recent_rejection",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_serious_sanctions",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF

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

  schema = <<EOF
[
    {
        "name": "tdoc_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "remaining_criteria_needed",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "compliant_reporting_eligible",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_time_on_supervision_level",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_drug_screen",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_fines_fees",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_recent_rejection",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "almost_eligible_serious_sanctions",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF
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

  schema = <<EOF
[
    {
        "name": "state_code",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "officer_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "metric_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "period",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "end_date",
        "type": "DATE",
        "mode": "NULLABLE"
    },
    {
        "name": "metric_rate",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },
    {
        "name": "caseload_type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "target",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },
    {
        "name": "threshold",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },
    {
        "name": "status",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF

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

  schema = <<EOF
[
    {
        "name": "state_code",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "external_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "staff_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "full_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "pseudonymized_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "supervision_district",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "email",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF

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

  schema = <<EOF
[
    {
        "name": "state_code",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "external_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "staff_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "full_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "pseudonymized_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "supervisor_external_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "supervision_district",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "specialized_caseload_type",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
EOF

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
    source_uris           = ["gs://${var.project_id}-snooze-statuses/*.json"]
  }

  schema = <<EOF
[
    {
        "name": "state_code",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "person_external_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "opportunity_type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "snoozed_by",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "snooze_start_date",
        "type": "DATE",
        "mode": "NULLABLE"
    },
    {
        "name": "snooze_end_date",
        "type": "DATE",
        "mode": "NULLABLE"
    },
    {
        "name": "denial_reasons",
        "type": "STRING",
        "mode": "REPEATED"
    },
    {
        "name": "other_reason",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "as_of",
        "type": "DATE",
        "mode": "NULLABLE"
    }
]
EOF

}