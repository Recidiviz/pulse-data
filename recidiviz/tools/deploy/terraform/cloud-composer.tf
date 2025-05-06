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

data "google_secret_manager_secret_version" "airflow_sendgrid_api_key" {
  secret = "airflow_sendgrid_api_key"
}

resource "google_secret_manager_secret" "recidiviz_app_engine_image" {
  # In order to use a secret as an Airflow variable, it must be prefixed with "airflow-variables-"
  # https://cloud.google.com/composer/docs/composer-2/configure-secret-manager#add
  secret_id = "airflow-variables-RECIDIVIZ_APP_ENGINE_IMAGE"
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
    }
  }
}

resource "google_secret_manager_secret_version" "recidiviz_app_engine_image" {
  secret      = google_secret_manager_secret.recidiviz_app_engine_image.name
  secret_data = "us-docker.pkg.dev/${var.project_id}/appengine/default:${var.docker_image_tag}"
}

resource "google_secret_manager_secret" "data_platform_version" {
  secret_id = "airflow-variables-DATA_PLATFORM_VERSION"
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
    }
  }
}

resource "google_secret_manager_secret_version" "data_platform_version" {
  secret      = google_secret_manager_secret.data_platform_version.name
  secret_data = var.docker_image_tag
}

data "google_compute_default_service_account" "default" {
  project = var.project_id
}

resource "google_composer_environment" "default_v2" {
  provider = google-beta
  name     = "orchestration-v2"
  region   = var.us_central_region

  config {

    software_config {
      airflow_config_overrides = {
        "api-auth_backends"                        = "airflow.composer.api.backend.composer_auth,airflow.api.auth.backend.session"
        "api-composer_auth_user_registration_role" = "Op"
        # For most tasks, the time it takes to load the entire Airflow environment in a new subprocess is negligible
        # This should help with cross-process resource contention
        "core-execute_tasks_new_python_interpreter" = "True"
        # The default maximum is 1024, but there may be instances where we may have stopped
        # SFTP and will need to catch up after a few days, so we will increase the limit.
        "core-max_map_length" = 2000
        # Allow up to active 48 tasks per DAG
        "core-max_active_tasks_per_dag" = 48
        # Default execution timeout for all tasks, in seconds, set to 1 day. If a task 
        # instance runs longer than this value, it will be marked as failed. Setting this
        # ensures tasks that are zombie'd but still running do not run indefinitely.
        "core-default_task_execution_timeout"       = 86400
        "celery-worker_concurrency"                 = 16
        "email-email_backend"                       = "airflow.providers.sendgrid.utils.emailer.send_email"
        "email-email_conn_id"                       = "sendgrid_default"
        "webserver-rbac"                            = true
        "webserver-web_server_name"                 = "orchestration-v2"
        "webserver-show_trigger_form_if_no_params"  = "True"
        "scheduler-scheduler_zombie_task_threshold" = 3600
        "secrets-backend"                           = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
        "secrets-backend_kwargs"                    = "{\"connections_prefix\": \"airflow-connections\", \"sep\": \"-\"}"
      }
      env_variables = {
        # DO NOT add variables that change with any frequency here. Updating these values causes Airflow tasks
        # to be interrupted. Instead, add it as a secret like RECIDIVIZ_APP_ENGINE_IMAGE defined above.
        "RECIDIVIZ_ENV"        = var.project_id == "recidiviz-123" ? "production" : "staging"
        "SENDGRID_API_KEY"     = data.google_secret_manager_secret_version.airflow_sendgrid_api_key.secret_data,
        "SENDGRID_MAIL_FROM"   = var.project_id == "recidiviz-staging" ? "alerts+airflow-staging@recidiviz.org" : "alerts+airflow-production@recidiviz.org"
        "SENDGRID_MAIL_SENDER" = var.project_id == "recidiviz-staging" ? "Airflow Alerts (staging)" : "Airflow Alerts (production)"
      }
      pypi_packages = {
        "us"                              = "==3.1.1"
        "apache-airflow-providers-sftp"   = "==4.9.1"
        "python-levenshtein"              = "==0.25.1"
        "dateparser"                      = "==1.2.0"
        "apache-airflow-providers-github" = "==2.6.2"
        "pygithub"                        = "==2.5.0"
      }
      image_version = "composer-2.9.7-airflow-2.7.3"
    }

    private_environment_config {
      # Ensure that access to the public endpoint of the GKE cluster is denied
      enable_private_endpoint = true
    }

    workloads_config {
      scheduler {
        count      = 2
        cpu        = 4
        memory_gb  = 15
        storage_gb = 10
      }
      web_server {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 10
      }
      worker {
        cpu        = 4
        memory_gb  = 16
        storage_gb = 10
        min_count  = 2
        max_count  = 30
      }
      triggerer {
        count     = 1
        cpu       = 0.5
        memory_gb = 0.5
      }
    }

    node_config {
      service_account = data.google_compute_default_service_account.default.email
    }

  }
}
