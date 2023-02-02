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
  airflow_files_to_copy_to_bucket = toset(flatten([
    for path_sequence in yamldecode(file("${path.module}/config/cloud_composer_airflow_files_to_copy.yaml")) : [
      for file in fileset(replace(path_sequence[0], "recidiviz/", "${local.recidiviz_root}/"), path_sequence[1]) : "${path_sequence[0]}/${file}"
    ]
  ]))
  source_files_to_copy_to_bucket = toset(flatten([
    for path_sequence in yamldecode(file("${path.module}/config/cloud_composer_source_files_to_copy.yaml")) : [
      for file in fileset(replace(path_sequence[0], "recidiviz/", "${local.recidiviz_root}/"), path_sequence[1]) : "${path_sequence[0]}/${file}"
    ]
  ]))
}

resource "google_composer_environment" "default_v2" {
  name   = "orchestration-v2"
  region = var.region
  config {

    software_config {
      # TODO(#4900): Not sure if we actually need these, given that they are specified in airflow.cfg, but leaving them
      # for consistency with the existing dag for now.
      airflow_config_overrides = {
        "api-auth_backend"                         = "airflow.composer.api.backend.composer_auth"
        "api-composer_auth_user_registration_role" = "Op"
        "celery-worker_concurrency"                = 3
        "webserver-web_server_name"                = "orchestration-v2"
        "secrets-backend"                          = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
        "secrets-backend_kwargs"                   = "{\"connections_prefix\": \"airflow-connections\", \"sep\": \"-\"}"
      }
      env_variables = {
        "CONFIG_FILE" = "/home/airflow/gcs/dags/recidiviz/calculator/pipeline/calculation_pipeline_templates.yaml"
      }
      pypi_packages = {
        "us"                                = "==2.0.2"
        "apache-airflow-providers-sftp"     = "==4.2.0"
        "apache-airflow-providers-mysql"    = "==3.4.0"
        "apache-airflow-providers-postgres" = "==5.4.0"
        "python-levenshtein"                = "==0.20.9"
        "dateparser"                        = "==1.1.6"
      }
      image_version = "composer-2.1.4-airflow-2.4.3"
    }

    private_environment_config {
      # Ensure that access to the public endpoint of the GKE cluster is denied
      enable_private_endpoint = true
    }

    workloads_config {
      scheduler {
        count      = 1
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        min_count  = 2
        max_count  = 6
      }
    }

  }

}

resource "google_storage_bucket_object" "recidiviz_airflow_file" {
  for_each = local.airflow_files_to_copy_to_bucket
  name     = "dags/${trimprefix(each.key, "recidiviz/airflow/dags/")}"
  bucket   = local.composer_dag_bucket
  source   = "${local.recidiviz_root}/${trimprefix(each.key, "recidiviz/")}"
}

resource "google_storage_bucket_object" "recidiviz_source_file" {
  for_each = local.source_files_to_copy_to_bucket
  name     = "dags/${each.key}"
  bucket   = local.composer_dag_bucket
  source   = "${local.recidiviz_root}/${trimprefix(each.key, "recidiviz/")}"
}

resource "google_storage_bucket_object" "airflow_cfg" {
  name   = "airflow.cfg"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/airflow/airflow.cfg"
}
