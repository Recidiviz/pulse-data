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
}

# TODO(#11816) Remove when deployed Cloud Composer v2
resource "google_composer_environment" "default" {
  name   = "orchestration"
  region = var.region
  config {
    node_count = 5

    node_config {
      zone         = var.zone
      machine_type = "n1-standard-4"
      ip_allocation_policy {
        # Ensure that we use a VPC-native cluster
        use_ip_aliases = true
      }
    }

    software_config {
      # TODO(#4900): Not sure if we actually need these, given that they are specified in airflow.cfg, but leaving them
      # for consistency with the existing dag for now.
      airflow_config_overrides = {
        "api-auth_backend"          = "airflow.api.auth.backend.default"
        "webserver-web_server_name" = "orchestration"
      }
      env_variables = {
        "CONFIG_FILE" = "/home/airflow/gcs/dags/recidiviz/calculator/pipeline/calculation_pipeline_templates.yaml"
      }
      image_version  = "composer-1.17.0-airflow-1.10.15"
      python_version = "3"
    }

    private_environment_config {
      # Ensure that access to the public endpoint of the GKE cluster is denied
      enable_private_endpoint = true
    }

  }

}

resource "google_composer_environment" "default_v2" {
  name   = "orchestration-v2"
  region = var.region
  config {

    software_config {
      # TODO(#4900): Not sure if we actually need these, given that they are specified in airflow.cfg, but leaving them
      # for consistency with the existing dag for now.
      airflow_config_overrides = {
        "api-auth_backend"          = "airflow.api.auth.backend.default"
        "webserver-web_server_name" = "orchestration-v2"
      }
      env_variables = {
        "CONFIG_FILE" = "/home/airflow/gcs/dags/recidiviz/calculator/pipeline/calculation_pipeline_templates.yaml"
      }
      image_version = "composer-2.0.8-airflow-2.2.3"
    }

    private_environment_config {
      # Ensure that access to the public endpoint of the GKE cluster is denied
      enable_private_endpoint = true
    }
  }

}

resource "google_storage_bucket_object" "dags_file" {
  for_each = fileset("${local.recidiviz_root}/airflow/dags", "*dag*.py")
  name     = "dags/${each.key}"
  bucket   = local.composer_dag_bucket
  source   = "${local.recidiviz_root}/airflow/dags/${each.key}"
}

resource "google_storage_bucket_object" "operators_file" {
  for_each = fileset("${local.recidiviz_root}/airflow/dags/operators", "*.py")
  name     = "dags/operators/${each.key}"
  bucket   = local.composer_dag_bucket
  source   = "${local.recidiviz_root}/airflow/dags/operators/${each.key}"
}

resource "google_storage_bucket_object" "pipeline_templates" {
  name   = "dags/recidiviz/calculator/pipeline/calculation_pipeline_templates.yaml"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/calculator/pipeline/calculation_pipeline_templates.yaml"
}

resource "google_storage_bucket_object" "cloud_function_utils" {
  name   = "dags/recidiviz/cloud_functions/cloud_function_utils.py"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/cloud_functions/cloud_function_utils.py"
}

resource "google_storage_bucket_object" "airflow_cfg" {
  name   = "airflow.cfg"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/airflow/airflow.cfg"
}

resource "google_storage_bucket_object" "yaml_dict" {
  name   = "dags/recidiviz/utils/yaml_dict.py"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/utils/yaml_dict.py"
}
