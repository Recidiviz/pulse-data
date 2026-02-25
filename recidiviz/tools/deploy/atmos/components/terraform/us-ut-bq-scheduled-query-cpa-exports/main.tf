# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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

# Enable the data transfer service
resource "google_project_service" "data_transfer_service" {
  project                    = var.project_id
  service                    = "bigquerydatatransfer.googleapis.com"
  disable_dependent_services = false
  disable_on_destroy         = false
}

# Create a service account for the scheduled query
resource "google_service_account" "bigquery_scheduled_queries" {
  account_id   = "bq-scheduled-queries"
  display_name = "BigQuery Scheduled Queries Service Account"
  description  = "Account that runs BigQuery scheduled queries in ${var.project_id}."
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.bigquery_scheduled_queries.email}"
}

# Grant read access to the recidiviz-staging reentry dataset (source of client records)
resource "google_bigquery_dataset_iam_member" "staging_reentry_reader" {
  project    = "recidiviz-staging"
  dataset_id = "reentry"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.bigquery_scheduled_queries.email}"
}

# Grant read access to the recidiviz-rnd-planner reentry_db_recidiviz dataset (source of plan/generation/asset records)
resource "google_bigquery_dataset_iam_member" "rnd_planner_reentry_db_reader" {
  project    = "recidiviz-rnd-planner"
  dataset_id = "reentry_db_recidiviz"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.bigquery_scheduled_queries.email}"
}

# Create the destination dataset
resource "google_bigquery_dataset" "cpa_exports" {
  dataset_id  = "cpa_exports"
  project     = var.project_id
  location    = "US"
  description = "Dataset for CPA export tables including action plans."
}

# Scheduled query to materialize action plans
resource "google_bigquery_data_transfer_config" "action_plans" {
  display_name           = "action_plans"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = var.schedule
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = google_bigquery_dataset.cpa_exports.dataset_id
  project                = var.project_id

  params = {
    destination_table_name_template = "action_plans"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT
    c.external_id as client_id,
    p.created_at,
    p.updated_at,
    pg.markdown_result as action_plan,
    pg.created_at as generated_at,
    pg.gen_type as generation_type,
FROM `recidiviz-rnd-planner.reentry_db_recidiviz.plan` p
LEFT JOIN `recidiviz-staging.reentry.client` c
    ON p.client_pseudo_id = c.pseudonymized_id
LEFT JOIN `recidiviz-rnd-planner.reentry_db_recidiviz.plangeneration` pg
    ON p.id = pg.plan_id
WHERE c.state_code = 'US_UT'
EOT
  }

  depends_on = [
    google_project_service.data_transfer_service,
    google_project_iam_member.bigquery_admin,
  ]
}

# Scheduled query to materialize chatbot transcripts
resource "google_bigquery_data_transfer_config" "chatbot_transcripts" {
  display_name           = "chatbot_transcripts"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = var.schedule
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = google_bigquery_dataset.cpa_exports.dataset_id
  project                = var.project_id

  params = {
    destination_table_name_template = "chatbot_transcripts"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT
    c.external_id as client_id,
    p.created_at,
    p.updated_at,
    SAFE_CONVERT_BYTES_TO_STRING(pa.file_blob) as transcript,
    pa.uploaded_at,
FROM `recidiviz-rnd-planner.reentry_db_recidiviz.plan` p
LEFT JOIN `recidiviz-staging.reentry.client` c
    ON p.client_pseudo_id = c.pseudonymized_id
LEFT JOIN `recidiviz-rnd-planner.reentry_db_recidiviz.planasset` pa
    ON p.id = pa.plan_id
WHERE c.state_code = 'US_UT'
    AND pa.filename = 'messages.json'
EOT
  }

  depends_on = [
    google_project_service.data_transfer_service,
    google_project_iam_member.bigquery_admin,
  ]
}

# Scheduled query to materialize intake summaries
resource "google_bigquery_data_transfer_config" "intake_summaries" {
  display_name           = "intake_summaries"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = var.schedule
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = google_bigquery_dataset.cpa_exports.dataset_id
  project                = var.project_id

  params = {
    destination_table_name_template = "intake_summaries"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-EOT
SELECT
    c.external_id as client_id,
    p.created_at,
    p.updated_at,
    SAFE_CONVERT_BYTES_TO_STRING(pa.file_blob) as summary,
    pa.uploaded_at,
FROM `recidiviz-rnd-planner.reentry_db_recidiviz.plan` p
LEFT JOIN `recidiviz-staging.reentry.client` c
    ON p.client_pseudo_id = c.pseudonymized_id
LEFT JOIN `recidiviz-rnd-planner.reentry_db_recidiviz.planasset` pa
    ON p.id = pa.plan_id
WHERE c.state_code = 'US_UT'
    AND pa.filename = 'summary.md'
EOT
  }

  depends_on = [
    google_project_service.data_transfer_service,
    google_project_iam_member.bigquery_admin,
  ]
}
