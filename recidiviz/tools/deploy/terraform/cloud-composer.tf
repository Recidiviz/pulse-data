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
  temporary_directory = "${dirname(local.recidiviz_root)}/.tfout"
  # Transforms the dag_gcs_prefix output variable from composer into just the gcs bucket name. Output docs:
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.dag_gcs_prefix
  composer_dag_bucket = trimprefix(trimsuffix(google_composer_environment.default_v2.config.0.dag_gcs_prefix, "/dags"), "gs://")
  source_files_to_copy_to_bucket = toset(flatten([
    for path_sequence in yamldecode(file("${path.module}/config/cloud_composer_source_files_to_copy.yaml")) : [
      for file in fileset(replace(path_sequence[0], "recidiviz/", "${local.recidiviz_root}/"), path_sequence[1]) : "${path_sequence[0]}/${file}"
    ]
  ]))
}

data "google_secret_manager_secret_version" "airflow_sendgrid_api_key" {
  secret = "airflow_sendgrid_api_key"
}

data "external" "airflow_source_files" {
  working_dir = "${dirname(local.recidiviz_root)}/../.."
  program     = ["pipenv", "run", "python", "-m", "recidiviz.tools.airflow.get_airflow_source_files", "--dry-run", "False"]
}

resource "google_composer_environment" "default_v2" {
  provider = google-beta
  name     = "orchestration-v2"
  region   = var.region

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
        "core-max_map_length"                       = 2000
        "celery-worker_concurrency"                 = 16
        "email-email_backend"                       = "airflow.providers.sendgrid.utils.emailer.send_email"
        "email-email_conn_id"                       = "sendgrid_default"
        "webserver-rbac"                            = true
        "webserver-web_server_name"                 = "orchestration-v2"
        "scheduler-scheduler_zombie_task_threshold" = 3600
        "secrets-backend"                           = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
        "secrets-backend_kwargs"                    = "{\"connections_prefix\": \"airflow-connections\", \"sep\": \"-\"}"
      }
      env_variables = {
        "DATA_PLATFORM_VERSION"      = var.docker_image_tag
        "RECIDIVIZ_APP_ENGINE_IMAGE" = "us.gcr.io/${var.project_id}/appengine/default:${var.docker_image_tag}"
        "SENDGRID_API_KEY"           = data.google_secret_manager_secret_version.airflow_sendgrid_api_key.secret_data,
        "SENDGRID_MAIL_FROM"         = var.project_id == "recidiviz-staging" ? "alerts+airflow-staging@recidiviz.org" : "alerts+airflow-production@recidiviz.org"
        "SENDGRID_MAIL_SENDER"       = var.project_id == "recidiviz-staging" ? "Airflow Alerts (staging)" : "Airflow Alerts (production)"
      }
      pypi_packages = {
        "us"                            = "==2.0.2"
        "apache-airflow-providers-sftp" = "==4.9.0"
        "python-levenshtein"            = "==0.25.0"
        "dateparser"                    = "==1.2.0"
      }
      image_version = "composer-2.6.2-airflow-2.6.3"
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

  }

}

resource "google_storage_bucket_object" "recidiviz_source_file" {
  for_each = toset(keys(data.external.airflow_source_files.result))
  name     = "dags/${data.external.airflow_source_files.result[each.key]}"
  bucket   = local.composer_dag_bucket
  source   = "${local.recidiviz_root}/${trimprefix(each.key, "recidiviz/")}"
}
