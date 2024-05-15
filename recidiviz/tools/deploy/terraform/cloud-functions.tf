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

data "google_secret_manager_secret_version" "sendgrid_api_key" {
  secret = "sendgrid_api_key"
}

data "google_secret_manager_secret_version" "po_report_cdn_static_ip" {
  secret = "po_report_cdn_static_IP"
}

resource "google_cloudfunctions_function" "trigger_calculation_dag" {
  name          = "trigger_calculation_dag"
  runtime       = "python38"
  max_instances = 3000

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/${var.project_id}/topics/v1.calculator.trigger_calculation_pipelines"
  }

  entry_point = "trigger_calculation_dag"
  environment_variables = {
    # This is an output variable from the composer environment, relevant docs:
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
    "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }
}



resource "google_cloudfunctions_function" "trigger_hourly_monitoring_dag" {
  name          = "trigger_hourly_monitoring_dag"
  runtime       = "python38"
  max_instances = 3000

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/${var.project_id}/topics/v1.airflow_monitoring.trigger_hourly_monitoring_dag"
  }

  entry_point = "trigger_hourly_monitoring_dag"
  environment_variables = {
    # This is an output variable from the composer environment, relevant docs:
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
    "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }
}

resource "google_cloudfunctions_function" "trigger_sftp_dag" {
  name          = "trigger_sftp_dag"
  runtime       = "python38"
  max_instances = 3000

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.sftp_pubsub_topic.id
  }

  entry_point = "trigger_sftp_dag"
  environment_variables = {
    # This is an output variable from the composer environment, relevant docs:
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
    "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }
}

resource "google_cloudfunctions_function" "trigger_ingest_dag" {
  name          = "trigger_ingest_dag"
  runtime       = "python38"
  max_instances = 3000

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.ingest_dag_pubsub_topic.id
  }

  entry_point = "trigger_ingest_dag"
  environment_variables = {
    # This is an output variable from the composer environment, relevant docs:
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
    "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }
}

resource "google_cloudfunctions_function" "trigger_raw_data_import_dag" {
  name          = "trigger_raw_data_import_dag"
  runtime       = "python38"
  max_instances = 3000

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.raw_data_import_dag_pubsub_topic.id
  }

  entry_point = "trigger_raw_data_import_dag"
  environment_variables = {
    # This is an output variable from the composer environment, relevant docs:
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
    "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = local.repo_url
  }
}
