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

# On weekdays, we want to run the calc DAG twice: once at 6 am EST to have data fresh for 
# the start of the east coast work day (9 am EST) and once at 7 am PST for the start of 
# the west coast work day (10 am PST, to account for when files actually arrive)
resource "google_cloud_scheduler_job" "schedule_incremental_calculation_pipeline_weekday_run_topic" {
  name = "schedule_calculation_dag_weekday_run_cloud_function"
  # in prod, at 3 AM and 7 AM PST on weekdays. in staging just at 3 AM
  schedule    = local.is_production ? "0 3,7 * * 1-5" : "0 3 * * 1-5"
  description = "Triggers the calculation DAG via pubsub"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    # topic's full resource name.
    topic_name = "projects/${var.project_id}/topics/v1.calculator.trigger_calculation_pipelines"
    # Run nightly DAG with no state_code filter.
    data = base64encode("{\"ingest_instance\": \"PRIMARY\"}")
  }
}

# On weekends, we only need to run this calc DAG once per day.
resource "google_cloud_scheduler_job" "schedule_incremental_calculation_pipeline_weekend_topic" {
  name = "schedule_calculation_dag_weekend_run_cloud_function"
  # at 3 AM PST on weekends
  schedule    = "0 3 * * 6,7"
  description = "Triggers the calculation DAG via pubsub"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    # topic's full resource name.
    topic_name = "projects/${var.project_id}/topics/v1.calculator.trigger_calculation_pipelines"
    # Run nightly DAG with no state_code filter.
    data = base64encode("{\"ingest_instance\": \"PRIMARY\"}")
  }
}

resource "google_cloud_scheduler_job" "schedule_airflow_hourly_monitoring_dag_run_topic" {
  name        = "schedule_airflow_hourly_monitoring_dag_run_cloud_function"
  schedule    = "0 * * * *" # Every hour at the 0 minute
  description = "Schedules the running of the hourly monitoring DAG pipeline topic"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    topic_name = google_pubsub_topic.airflow_monitoring_topic.id
    data       = base64encode("DATA") # Added to fulfill requirements that data has to be passed
  }
}

resource "google_cloud_scheduler_job" "schedule_sftp_dag_run_topic" {
  name        = "schedule_sftp_dag_run_cloud_function"
  schedule    = "45 * * * *" # Every hour at the 45 minute
  description = "Schedules the running of the SFTP DAG pipeline topic"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    topic_name = google_pubsub_topic.sftp_pubsub_topic.id
    data       = base64encode("DATA") # Added to fulfill requirements that data has to be passed
  }
}

# TODO(#38729): make dag scheduling more nuanced to ensure that raw data is always
# as fresh as possible

# On weekdays, we want to run the raw data import DAG twice: once at 5 am EST to have data
# fresh for the start of the "east coast work day" calc DAG (6 am EST) and once at 
# 6 am PST to have data fresh for the "west coast work day" calc DAG" (7 am PST)
resource "google_cloud_scheduler_job" "schedule_raw_data_import_dag_weekday_run_topic" {
  name = "schedule_raw_data_import_dag_weekday_run_cloud_function"
  # in prod, at 2 AM and 6 AM PST on weekdays. in staging just at 2 AM
  schedule    = local.is_production ? "0 2,6 * * 1-5" : "0 2 * * 1-5"
  description = "Triggers the raw data import DAG via pubsub"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    topic_name = google_pubsub_topic.raw_data_import_dag_pubsub_topic.id
    # Run raw data import DAG with a PRIMARY filter
    data = base64encode("{\"ingest_instance\": \"PRIMARY\"}")
  }
}

resource "google_cloud_scheduler_job" "schedule_raw_data_import_dag_weekend_run_topic" {
  name = "schedule_raw_data_import_dag_weekend_run_cloud_function"
  # at 2 AM on weekends
  schedule    = "0 2 * * 6,7"
  description = "Triggers the raw data import DAG via pubsub"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    topic_name = google_pubsub_topic.raw_data_import_dag_pubsub_topic.id
    # Run raw data import DAG with a PRIMARY filter
    data = base64encode("{\"ingest_instance\": \"PRIMARY\"}")
  }
}

resource "google_cloud_scheduler_job" "prune_old_dataflow_data" {
  name        = "prune-old-dataflow-data"
  schedule    = "0 0 * * *" # Every day at 00:00
  description = "Move old Dataflow metric output to cold storage"
  time_zone   = "America/Los_Angeles"
  # TODO(#27436) Speed up pruning and lower the attempt_deadline
  attempt_deadline = "900s" # 15 minutes

  retry_config {
    min_backoff_duration = "2.500s"
    max_doublings        = 5
  }

  http_target {
    uri         = "https://${var.project_id}.appspot.com/calculation_data_storage_manager/prune_old_dataflow_data"
    http_method = "GET"

    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      audience              = local.app_engine_iap_client
    }
  }
}

resource "google_cloud_scheduler_job" "delete_empty_bq_datasets" {
  name             = "delete-empty-bq-datasets"
  schedule         = "0 0 * * *" # Every day at 00:00
  description      = "Delete empty datasets in BigQuery"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "2.500s"
    max_doublings        = 5
  }

  http_target {
    uri         = "https://${var.project_id}.appspot.com/calculation_data_storage_manager/delete_empty_or_temp_datasets"
    http_method = "GET"

    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      audience              = local.app_engine_iap_client
    }
  }
}

resource "google_cloud_scheduler_job" "update_long_term_backups" {
  name = "update-long-term-backups"
  # Runs at a time when it's unlikely someone will be running the flashing checklist, to avoid
  # 'Operation failed because another operation was already in progress' errors.
  schedule         = "0 23 * * 1" # Every Monday 23:00
  description      = "Create new long-term backup and delete oldest long-term backup"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "2.500s"
    max_doublings        = 5
  }

  http_target {
    uri         = "${local.application_data_import_url}/backup_manager/update_long_term_backups"
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.application_data_import_cloud_run.email
    }
  }
}


resource "google_cloud_scheduler_job" "hydrate_admin_panel_cache" {
  name             = "hydrate-admin-panel-cache"
  schedule         = "*/15 * * * *" # Every 15 minutes
  description      = "[Admin Panel] Hydrate cache"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "30s"
    max_doublings        = 5
  }

  # when this cron job runs, create and run a Batch job
  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.admin_panel_hydrate_cache.name}:run"

    headers = {
      "Content-Type" = "application/json"
      "User-Agent"   = "Google-Cloud-Scheduler"
    }

    oauth_token {
      service_account_email = google_service_account.admin_panel_cloud_run.email
    }
  }
}

locals {
  # Found at https://console.cloud.google.com/apis/credentials (IAP-admin-panel-load-balancer-backend-default)
  cloud_run_iap_client = local.is_production ? "688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.googleusercontent.com" : "984160736970-4vg3gpqmskvpkhqim39b8kp8e4ommu94.apps.googleusercontent.com"
}
