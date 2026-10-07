# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
# Hosted eOMIS writeback (OBT-37548): the eOMIS writeback entrypoint run as a
# scheduled Cloud Run Job, one job per flow.

locals {
  # Same CO job in both projects, so staging rehearses production. Dry run
  # unless the execution adds --commit.
  eomis_writeback_us_co_job = {
    args = [
      "--flow=co_work",
      "--flow=co_cert",
      "--flow=co_ged",
      "--flow=co_edovo",
      # CDOC's service account; its login needs no browser.
      "--account=svc",
      "--workers=8",
      # Above the first run's candidate count.
      "--max-writes=30000",
    ]
    # Room for the first run.
    timeout = "86400s"
    # Before CDOC's 8:30 PM Mountain export.
    schedule  = "0 18 * * *"
    time_zone = "America/Denver"
    env       = {}
    paused    = true
  }

  eomis_writeback_env_config = {
    staging = {
      # One entry per flow. Drives the jobs, scheduler, and per-job IAM. The
      # args are appended to the module invocation; dry-run is the absence of
      # --commit.
      jobs = {
        # GED and Edovo share one job and eOMIS login session. Starts paused.
        # TODO(AR-490): Set US_AR_EDOVO_POLICY_START_MONTH to the confirmed
        # start month before unpausing. Edovo cannot run without it.
        "us-ar-ged" = {
          args      = ["--flow=ar_ged", "--flow=ar_edovo"]
          schedule  = "0 0 * * *"
          time_zone = "Etc/UTC"
          env       = {}
        }
        # TODO(OBT-22951): unpause only after CDOC confirms the cadence.
        "us-co-continuous" = local.eomis_writeback_us_co_job
      }
      # Which eOMIS instance/secrets the running container targets.
      recidiviz_env = "staging"
    }
    production = {
      jobs = {
        "us-ar-ged" = {
          args      = ["--flow=ar_ged", "--commit", "--allow-prod-write"]
          schedule  = "0 0 * * *"
          time_zone = "Etc/UTC"
          env       = {}
          paused    = true
        }
        "us-co-continuous" = local.eomis_writeback_us_co_job
      }
      recidiviz_env = "production"
    }
  }

  eomis_writeback = local.eomis_writeback_env_config[local.is_production ? "production" : "staging"]

  eomis_writeback_enabled  = length(local.eomis_writeback.jobs) > 0
  eomis_writeback_sa_email = one(google_service_account.eomis_writeback[*].email)

  eomis_writeback_runtime_roles = [
    "roles/bigquery.jobUser",
    # Staging-scoped read for the candidate view; tighten to the productized
    # dataset once the source view graduates out of scratch (#89040).
    "roles/bigquery.dataViewer",
    "roles/logging.logWriter",
  ]
}

# Service account for the eOMIS writeback Cloud Run job.
resource "google_service_account" "eomis_writeback" {
  count        = local.eomis_writeback_enabled ? 1 : 0
  account_id   = "eomis-writeback"
  display_name = "eOMIS Writeback Cloud Run Service Account"
  description  = <<EOT
Runtime identity for the hosted eOMIS writeback Cloud Run job(s). Least-privilege:
BigQuery read for candidate sourcing, append access to the eomis_writeback_metadata
audit ledger, and access to the per-state eOMIS secrets only. Managed in Terraform.
EOT
}

resource "google_project_iam_member" "eomis_writeback_runtime_iam" {
  for_each = local.eomis_writeback_enabled ? toset(local.eomis_writeback_runtime_roles) : toset([])
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${local.eomis_writeback_sa_email}"
}

# Read access to the configs bucket, where the writeback kill switch's pause
# markers live. Bucket-scoped: the job needs one prefix, not project-wide storage.
# Without it the kill-switch check 403s and every run dies at startup.
#
# objectViewer is sufficient because the check *lists* the marker prefix
# (storage.objects.list) rather than fetching bucket metadata. Reading the bucket
# itself would need storage.buckets.get, which this role does not carry.
resource "google_storage_bucket_iam_member" "eomis_writeback_configs_reader" {
  count  = local.eomis_writeback_enabled ? 1 : 0
  bucket = module.configs.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${local.eomis_writeback_sa_email}"
}

# Streaming-insert access for the audit ledger, scoped to that dataset only.
resource "google_bigquery_dataset_iam_member" "eomis_writeback_audit_ledger_editor" {
  count      = local.eomis_writeback_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = module.terraform_managed_bigquery_dataset["eomis_writeback_metadata"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.eomis_writeback_sa_email}"
}

# Secret-level access: only the specific eOMIS credentials, not all project secrets.
# The CO entries are the service account CDOC provisioned; they hold the login
# the scheduled CO job uses.
resource "google_secret_manager_secret_iam_member" "eomis_writeback_secret_access" {
  for_each = local.eomis_writeback_enabled ? toset([
    "eomis_us_ar_username",
    "eomis_us_ar_password",
    "eomis_us_co_svc_username",
    "eomis_us_co_svc_password",
  ]) : toset([])
  secret_id = each.key
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.eomis_writeback_sa_email}"
}

resource "google_cloud_run_v2_job" "eomis_writeback" {
  for_each = local.eomis_writeback.jobs
  name     = "eomis-writeback-${each.key}"
  location = var.us_central_region
  provider = google-beta

  template {
    task_count = 1
    template {
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      service_account       = local.eomis_writeback_sa_email
      max_retries           = 0
      # Above the 600s default: the runner paces per candidate with no wall-clock
      # cap. A job that works a large population sets its own; see the CO entry.
      timeout = lookup(each.value, "timeout", "3600s")
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["uv"]
        # --no-sync since the image already contains locked dependencies.
        args = concat(
          ["run", "--no-sync", "python", "-m", "recidiviz.entrypoints.eomis_writeback"],
          each.value.args,
        )
        # RECIDIVIZ_ENV determines which eOMIS instance and secrets the job uses.
        env {
          name  = "RECIDIVIZ_ENV"
          value = local.eomis_writeback.recidiviz_env
        }
        # Non-secret settings for each job. Credentials stay in Secret Manager.
        dynamic "env" {
          for_each = each.value.env
          content {
            name  = env.key
            value = env.value
          }
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
      }
      # Route all egress through the VPC connector and Cloud NAT so the job's
      # traffic sources from the static external-system-outbound-requests IP —
      # the same path production will need for the AR eOMIS IP allowlist.
      vpc_access {
        connector = google_vpc_access_connector.us_central_redis_vpc_connector.id
        egress    = "ALL_TRAFFIC"
      }
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

# Paused unless the job sets `paused = false`.
resource "google_cloud_scheduler_job" "eomis_writeback" {
  for_each         = local.eomis_writeback.jobs
  name             = "eomis-writeback-${each.key}"
  schedule         = each.value.schedule
  time_zone        = each.value.time_zone
  paused           = lookup(each.value, "paused", true)
  description      = "[eOMIS writeback] Trigger the ${each.key} writeback Cloud Run job"
  attempt_deadline = "320s"

  retry_config {
    min_backoff_duration = "15s"
    max_doublings        = 5
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.us_central_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.eomis_writeback[each.key].name}:run"

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.cloud_scheduler.email
    }
  }
}

# Job-level run.invoker for the Scheduler identity, not a project-wide grant.
resource "google_cloud_run_v2_job_iam_member" "eomis_writeback_scheduler_invoker" {
  for_each = local.eomis_writeback.jobs
  provider = google-beta
  project  = var.project_id
  location = var.us_central_region
  name     = google_cloud_run_v2_job.eomis_writeback[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_scheduler.email}"
}
