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

resource "google_cloud_scheduler_job" "schedule_incremental_calculation_pipeline_topic" {
  name        = "schedule_incremental_calculation_pipeline_cloud_function"
  schedule    = "0 6 * * *" # Every day at 6 am
  description = "Schedules the running of the incremental calculation pipeline topic"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    # topic's full resource name.
    topic_name = "projects/${var.project_id}/topics/v1.calculator.trigger_calculation_pipelines"
    # Run nightly DAG with no state_code filter.
    data = base64encode("{\"ingest_instance\": \"PRIMARY\", \"trigger_ingest_dag_post_bq_refresh\": false}")
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


resource "google_cloud_scheduler_job" "schedule_ingest_dag_run_topic" {
  name        = "schedule_ingest_dag_run_cloud_function"
  schedule    = "0 1 * * *" # Every day at 1 am Pacific
  description = "Triggers the ingest DAG via pubsub"
  time_zone   = "America/Los_Angeles"

  pubsub_target {
    topic_name = google_pubsub_topic.ingest_dag_pubsub_topic.id
    data       = base64encode("{}") # Run ingest dag with no filters.
  }
}

resource "google_cloud_scheduler_job" "prune_old_dataflow_data" {
  name             = "prune-old-dataflow-data"
  schedule         = "0 0 * * *" # Every day at 00:00
  description      = "Move old Dataflow metric output to cold storage"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

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
  # TODO(#20930): Delete above comment which will no longer be relevant when
  # ingest is moved to Dataflow and the flashing checklist no longer runs CloudSQL
  # operations.
  schedule         = "0 23 * * 1" # Every Monday 23:00
  description      = "Create new long-term backup and delete oldest long-term backup"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "2.500s"
    max_doublings        = 5
  }

  http_target {
    uri         = "https://${var.project_id}.appspot.com/backup_manager/update_long_term_backups"
    http_method = "GET"

    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      audience              = local.app_engine_iap_client
    }
  }
}

resource "google_cloud_scheduler_job" "ensure_all_raw_paths_normalized" {
  name             = "ensure-all-raw-paths-normalized"
  schedule         = "0 4 * * *" # Every day 4:00
  description      = "[Direct ingest] Check for unnormalized files in all regions"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "2.500s"
    max_doublings        = 5
  }

  http_target {
    uri         = "https://${var.project_id}.appspot.com/direct/ensure_all_raw_file_paths_normalized"
    http_method = "POST"

    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      audience              = local.app_engine_iap_client
    }
  }
}

resource "google_cloud_scheduler_job" "check_region_outstanding_work" {
  name             = "check-region-outstanding-work"
  schedule         = "0 * * * *" # Every hour at minute 0
  description      = "[Direct ingest] Check all regions for outstanding work"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "600s" # 10 minutes

  retry_config {
    min_backoff_duration = "30s"
    max_doublings        = 5
  }

  http_target {
    uri         = "https://${var.project_id}.appspot.com/direct/heartbeat"
    http_method = "POST"

    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      audience              = local.app_engine_iap_client
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
    uri         = "https://${var.app_engine_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.admin_panel_recalculate_stores.name}/jobs:run"

    headers = {
      "Content-Type" = "application/json"
      "User-Agent"   = "Google-Cloud-Scheduler"
    }

    oidc_token {
      service_account_email = google_service_account.admin_panel_cloud_run.email
      audience              = local.cloud_run_iap_client
    }
  }
}

locals {
  # Found at https://console.cloud.google.com/apis/credentials (IAP-admin-panel-load-balancer-backend-default)
  cloud_run_iap_client = local.is_production ? "688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.googleusercontent.com" : "984160736970-4vg3gpqmskvpkhqim39b8kp8e4ommu94.apps.googleusercontent.com"
}
