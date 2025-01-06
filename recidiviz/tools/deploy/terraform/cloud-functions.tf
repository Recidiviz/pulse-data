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

resource "google_cloudfunctions2_function" "trigger_calculation_dag" {
  name     = "trigger_calculation_dag"
  location = "us-central1"
  build_config {
    runtime     = "python311"
    entry_point = "trigger_calculation_dag"
    environment_variables = {
      # Hacky workaround since source directory option is broken https://issuetracker.google.com/issues/248110968
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
      GOOGLE_FUNCTION_SOURCE             = "recidiviz/cloud_functions/main.py"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  service_config {
    max_instance_count = 3000
    ingress_settings   = "ALLOW_INTERNAL_ONLY"
    environment_variables = {
      # This is an output variable from the composer environment, relevant docs:
      # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
      "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
      "GCP_PROJECT" = var.project_id
    }
  }

  event_trigger {
    event_type   = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = "projects/${var.project_id}/topics/v1.calculator.trigger_calculation_pipelines"
  }
}

resource "google_cloudfunctions2_function" "trigger_hourly_monitoring_dag" {
  name     = "trigger_hourly_monitoring_dag"
  location = "us-central1"
  build_config {
    runtime     = "python311"
    entry_point = "trigger_hourly_monitoring_dag"
    environment_variables = {
      # Hacky workaround since source directory option is broken https://issuetracker.google.com/issues/248110968
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
      GOOGLE_FUNCTION_SOURCE             = "recidiviz/cloud_functions/main.py"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  service_config {
    max_instance_count = 3000
    ingress_settings   = "ALLOW_INTERNAL_ONLY"
    environment_variables = {
      # This is an output variable from the composer environment, relevant docs:
      # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
      "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
      "GCP_PROJECT" = var.project_id
    }
  }

  event_trigger {
    event_type   = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = "projects/${var.project_id}/topics/v1.airflow_monitoring.trigger_hourly_monitoring_dag"
  }
}

resource "google_cloudfunctions2_function" "trigger_sftp_dag" {
  name     = "trigger_sftp_dag"
  location = "us-central1"
  build_config {
    runtime     = "python311"
    entry_point = "trigger_sftp_dag"
    environment_variables = {
      # Hacky workaround since source directory option is broken https://issuetracker.google.com/issues/248110968
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
      GOOGLE_FUNCTION_SOURCE             = "recidiviz/cloud_functions/main.py"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type   = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = google_pubsub_topic.sftp_pubsub_topic.id
  }

  service_config {
    max_instance_count = 3000
    ingress_settings   = "ALLOW_INTERNAL_ONLY"
    environment_variables = {
      # This is an output variable from the composer environment, relevant docs:
      # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
      "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
      "GCP_PROJECT" = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "trigger_raw_data_import_dag" {
  name     = "trigger_raw_data_import_dag"
  location = "us-central1"
  build_config {
    runtime     = "python311"
    entry_point = "trigger_raw_data_import_dag"
    environment_variables = {
      # Hacky workaround since source directory option is broken https://issuetracker.google.com/issues/248110968
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
      GOOGLE_FUNCTION_SOURCE             = "recidiviz/cloud_functions/main.py"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  service_config {
    max_instance_count = 3000
    ingress_settings   = "ALLOW_INTERNAL_ONLY"
    environment_variables = {
      # This is an output variable from the composer environment, relevant docs:
      # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment#config.0.airflow_uri
      "AIRFLOW_URI" = google_composer_environment.default_v2.config.0.airflow_uri
      "GCP_PROJECT" = var.project_id
    }
  }

  event_trigger {
    event_type   = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = google_pubsub_topic.raw_data_import_dag_pubsub_topic.id
  }
}

resource "google_cloudfunctions2_function" "handle_zipfile" {
  name        = "ingest_zipfile_handler"
  location    = "us-central1"
  description = "Unzip ingest files for raw data buckets"
  build_config {
    runtime     = "python311"
    entry_point = "handle_zipfile"
    environment_variables = {
      GOOGLE_FUNCTION_SOURCE             = "recidiviz/cloud_functions/ingest_filename_normalization.py"
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  service_config {
    max_instance_count = 10
    available_memory   = "8G"
    timeout_seconds    = 540
    environment_variables = {
      PYTHONPATH = "/workspace"
      PROJECT_ID = var.project_id
    }
    ingress_settings = "ALLOW_INTERNAL_ONLY"
  }
}

resource "google_cloudfunctions2_function" "filename_normalization" {
  name        = "ingest_filename_normalization"
  location    = "us-central1"
  description = "Normalize ingest file names for raw data buckets"
  build_config {
    runtime     = "python311"
    entry_point = "normalize_filename"
    environment_variables = {
      # Hacky workaround since source directory option is broken https://issuetracker.google.com/issues/248110968
      GOOGLE_INTERNAL_REQUIREMENTS_FILES = "recidiviz/cloud_functions/requirements.txt"
      GOOGLE_FUNCTION_SOURCE             = "recidiviz/cloud_functions/ingest_filename_normalization.py"
    }
    source {
      repo_source {
        repo_name  = "github_Recidiviz_pulse-data"
        commit_sha = var.git_hash
      }
    }
  }

  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type   = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = google_pubsub_topic.raw_data_storage_notification_topic.id
    # Retry failed invocations with an exponential backoff for up to 24 hours.
    # If cloud function retries are disabled, we would not be able to rely on
    # the pub/sub retry mechanism as each invocation is treated as a success
    # and pub/sub messages are acknowledged even if an error occurs.
    retry_policy = "RETRY_POLICY_RETRY"
  }

  service_config {
    max_instance_count = 50
    available_memory   = "512M"
    timeout_seconds    = 540
    environment_variables = {
      PYTHONPATH                   = "/workspace" # directory recidiviz/ lives in
      ZIPFILE_HANDLER_FUNCTION_URL = google_cloudfunctions2_function.handle_zipfile.url
      PROJECT_ID                   = var.project_id
    }
    ingress_settings              = "ALLOW_INTERNAL_ONLY"
    vpc_connector                 = google_vpc_access_connector.cloud_function_vpc_connector.name
    vpc_connector_egress_settings = "ALL_TRAFFIC"
  }
}
