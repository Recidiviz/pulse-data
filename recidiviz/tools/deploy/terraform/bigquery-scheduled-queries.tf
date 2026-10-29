# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

# This sets up scheduled queries to materialize tables on a regular schedule, e.g.
# those that are sourced from Google Sheets.

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

resource "google_project_iam_member" "bigquery_datatransfer_admin" {
  project = var.project_id
  # See following link for why this role is required:
  # https://cloud.google.com/bigquery/docs/use-service-accounts#required_permissions 
  role   = "roles/bigquery.admin"
  member = "serviceAccount:${google_service_account.bigquery_scheduled_queries.email}"
}

resource "google_bigquery_data_transfer_config" "product_roster_archive" {
  display_name           = "product_roster_archive"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 03:00" # In UTC, gives us end of day in US
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = module.terraform_managed_bigquery_dataset["export_archives"].dataset_id

  params = {
    destination_table_name_template = "product_roster_archive"
    write_disposition               = "WRITE_APPEND"
    query                           = <<-EOT
SELECT CURRENT_DATE("US/Eastern") AS export_date, *
FROM `${var.project_id}.reference_views.product_roster`
EOT
  }
}

resource "google_bigquery_data_transfer_config" "normalized_state_hydration_archive" {
  display_name           = "normalized_state_hydration_archive"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every day 03:00" # In UTC, gives us end of day in US
  service_account_name   = google_service_account.bigquery_scheduled_queries.email
  destination_dataset_id = "hydration_archive"

  params = {
    destination_table_name_template = "normalized_state_hydration_archive"
    write_disposition               = "WRITE_APPEND"
    query                           = <<-EOT
SELECT CURRENT_DATE("US/Eastern") AS hydration_date, *
FROM `${var.project_id}.platform_kpis.normalized_state_hydration_live_snapshot`
EOT
  }
}

# Daily copy of the GitHub org webhook events that the security team's SIEM
# keeps for 400 days, read through the authorized view the security team
# shares for this purpose (it drops two of their own repos and org-level
# events). The query reads across projects, so the scheduled queries account
# needs roles/bigquery.dataViewer on recidiviz-security:github_events_share,
# granted on the security team's side.
# The 7-day lookback lets one run absorb late deliveries and a few days of
# silent failure, and the MERGE on delivery_id keeps reruns idempotent.
resource "google_bigquery_data_transfer_config" "github_events_archive_daily_merge" {
  display_name         = "github_events_archive_daily_merge"
  location             = "US"
  data_source_id       = "scheduled_query"
  schedule             = "every day 04:00" # UTC
  service_account_name = google_service_account.bigquery_scheduled_queries.email

  email_preferences {
    enable_failure_email = true
  }

  params = {
    query = <<-EOT
MERGE `${var.project_id}.${module.terraform_managed_bigquery_dataset["github_events_raw"].dataset_id}.github_events_archive` AS archive
USING (
  SELECT
    delivery_id,
    event_timestamp,
    event_timestamp_ts,
    github_event_type,
    repository_full_name,
    sender_login,
    action,
    webhook_payload,
    'github_events_v2' AS siem_source_table
  FROM `recidiviz-security.github_events_share.github_events_v2`
  WHERE DATE(event_timestamp_ts) >= DATE_SUB(CURRENT_DATE("UTC"), INTERVAL 7 DAY)
    -- The merge skips rows with no delivery id or no timestamp. Such a row
    -- cannot match on delivery_id, and inserting it would fail the REQUIRED
    -- columns and wedge every run until it ages out. The freshness check
    -- below reports those rows instead.
    AND delivery_id IS NOT NULL
    AND event_timestamp_ts IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY event_timestamp_ts) = 1
) AS siem
ON archive.delivery_id = siem.delivery_id
  AND DATE(archive.event_timestamp_ts) >= DATE_SUB(CURRENT_DATE("UTC"), INTERVAL 7 DAY)
WHEN NOT MATCHED THEN
  INSERT (delivery_id, event_timestamp, event_timestamp_ts, github_event_type,
          repository_full_name, sender_login, action, webhook_payload, siem_source_table)
  VALUES (siem.delivery_id, siem.event_timestamp, siem.event_timestamp_ts, siem.github_event_type,
          siem.repository_full_name, siem.sender_login, siem.action, siem.webhook_payload, siem.siem_source_table)
EOT
  }
}

# Freshness signal for the archive. The first ASSERT fails the run, and so
# sends the failure email, when no delivery has reached the archive in the last
# two days; a MERGE that succeeds with zero rows would otherwise go unnoticed.
# The second ASSERT fails when the SIEM window holds rows the merge skipped for
# a missing delivery id or timestamp, so those rows surface instead of vanishing.
resource "google_bigquery_data_transfer_config" "github_events_archive_freshness_check" {
  display_name         = "github_events_archive_freshness_check"
  location             = "US"
  data_source_id       = "scheduled_query"
  schedule             = "every day 06:00" # UTC, two hours after the merge
  service_account_name = google_service_account.bigquery_scheduled_queries.email

  email_preferences {
    enable_failure_email = true
  }

  params = {
    query = <<-EOT
ASSERT (
  SELECT MAX(event_timestamp_ts)
  FROM `${var.project_id}.${module.terraform_managed_bigquery_dataset["github_events_raw"].dataset_id}.github_events_archive`
) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AS 'github_events_archive is stale: no webhook delivery archived in the last 2 days';
ASSERT (
  SELECT COUNT(*)
  FROM `recidiviz-security.github_events_share.github_events_v2`
  WHERE DATE(event_timestamp_ts) >= DATE_SUB(CURRENT_DATE("UTC"), INTERVAL 7 DAY)
    AND (delivery_id IS NULL OR event_timestamp_ts IS NULL)
) = 0
  AS 'github_events_share.github_events_v2 has rows with no delivery_id or event_timestamp_ts in the last 7 days; the daily merge skipped them';
EOT
  }
}
