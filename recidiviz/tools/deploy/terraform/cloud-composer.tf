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
  recidiviz_root      = "${path.root}/../../.."
  temporary_directory = "${dirname(local.recidiviz_root)}/.tfout"
  # Transforms the dag_gcs_prefix output variable from composer into just the gcs bucket name. Output docs:
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.dag_gcs_prefix
  composer_dag_bucket = trimprefix(trimsuffix(google_composer_environment.default.config.0.dag_gcs_prefix, "/dags"), "gs://")
}

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
        "CONFIG_FILE" = "/home/airflow/gcs/dags/production_calculation_pipeline_templates.yaml"
        # TODO(#4900): I think we get 'GCP_PROJECT' by default, so we can probably clean this up.
        "GCP_PROJECT_ID" = var.project_id
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

# This gets the IAP client id for the given airflow instance. It should only change when a composer environment is
# recreated. See https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf#getting_the_client_id.
data "external" "composer_iap_client_id" {
  program = ["python", "${path.module}/iap_client.py"]

  query = {
    airflow_uri = google_composer_environment.default.config.0.airflow_uri
  }
  # Outputs a result that contains 'iap_client_id' to be consumed by any resources that need to call into Airflow.
}

data "archive_file" "dags" {
  type        = "zip"
  output_path = "${local.temporary_directory}/dag.zip"
  source_dir  = "${local.recidiviz_root}/airflow/dag"
}

# We zip the files mostly because it isn't totally clear how to copy a directory to GCS. Airflow will unzip it on its
# own, so this works just fine.
resource "google_storage_bucket_object" "dags_archive" {
  name   = "dags/dag.zip"
  bucket = local.composer_dag_bucket
  source = data.archive_file.dags.output_path
}

resource "google_storage_bucket_object" "pipeline_templates" {
  name   = "dags/production_calculation_pipeline_templates.yaml"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/calculator/pipeline/production_calculation_pipeline_templates.yaml"
}

resource "google_storage_bucket_object" "cloud_function_utils" {
  name   = "dags/cloud_function_utils.py"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/cloud_functions/cloud_function_utils.py"
}

resource "google_storage_bucket_object" "airflow_cfg" {
  name   = "airflow.cfg"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/airflow/airflow.cfg"
}

resource "google_storage_bucket_object" "yaml_dict" {
  name   = "dags/yaml_dict.py"
  bucket = local.composer_dag_bucket
  source = "${local.recidiviz_root}/utils/yaml_dict.py"
}
