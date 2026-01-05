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
resource "google_cloud_run_v2_job" "admin_panel_hydrate_cache" {
  name     = "admin-panel-hydrate-cache"
  location = var.us_central_region
  provider = google-beta

  template {
    template {
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["uv"]
        args    = ["run", "python", "-m", "recidiviz.admin_panel.entrypoints.hydrate_cache"]
        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
        # This is the expected path for Cloud SQL Unix sockets in most, if not all, GCP runtimes
        # as defined here: https://cloud.google.com/sql/docs/postgres/connect-run#public-ip-default_1
        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "768Mi"
          }
        }
      }
      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [module.operations_database_v2.connection_name]
        }
      }
      vpc_access {
        connector = google_vpc_access_connector.us_central_redis_vpc_connector.id
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }
  }


  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}



resource "google_cloud_run_v2_job" "utah_data_transfer_sync" {
  name     = "utah-data-transfer-sync"
  location = var.us_central_region
  provider = google-beta

  template {
    template {
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["uv"]
        args    = ["run", "python", "-m", "recidiviz.tools.ingest.regions.us_ut.sync_bq_mirror_to_ingest_bucket", "--dry-run", "False"]
        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
      }
      max_retries = 0
    }
  }


  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

locals {
  jii_jobs = {
    "id-lsu-jii-initial-texts" = {
      "id"   = "us-central1/id-lsu-jii-initial-texts"
      "args" = ["run", "python", "-m", "recidiviz.case_triage.jii.send_jii_texts", "--message-type=initial_text", "--bigquery-view=", "--dry-run=False"]
    }
    "id-lsu-jii-eligibility-texts" = {
      "id"   = "us-central1/id-lsu-jii-eligibility-texts"
      "args" = ["run", "python", "-m", "recidiviz.case_triage.jii.send_jii_texts", "--message-type=eligibility_text", "--bigquery-view=", "--dry-run=False"]
    }
    "id-lsu-jii-update-statuses" = {
      "id"   = "us-central1/id-lsu-jii-update-statuses"
      "args" = ["run", "python", "-m", "recidiviz.case_triage.jii.send_jii_texts", "--message-type=initial_text", "--bigquery-view=", "--dry-run=True", "--redeliver-failed-messages=False", "--previous-batch-id-to-update-status-for="]
    }
    "redeliver-id-lsu-jii-initial-texts" = {
      "id"   = "us-central1/redeliver-id-lsu-jii-initial-texts"
      "args" = ["run", "python", "-m", "recidiviz.case_triage.jii.send_jii_texts", "--message-type=initial_text", "--bigquery-view=", "--dry-run=False", "--redeliver-failed-messages=True", "--previous-batch-id-to-update-status-for="]
    }
  }
}

resource "google_cloud_run_v2_job" "jii_jobs" {
  for_each = local.jii_jobs
  name     = each.key
  location = var.us_central_region
  provider = google-beta

  template {
    task_count = 1
    template {
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      max_retries           = 3
      service_account       = google_service_account.cloud_run.email
      timeout               = "600s"
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        args    = each.value.args
        command = ["uv"]
        name    = "default-1"
        env {
          name  = "RECIDIVIZ_ENV"
          value = "staging"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
      }
    }
  }
}
