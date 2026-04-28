resource "google_service_account" "trigger_dag_cloud_run" {
  account_id   = "dag-trigger-service-account"
  display_name = "Cloud Run Job Service Account for Airflow DAG triggering"
  description  = <<EOT
Service Account that acts as the identity for the Case Triage Cloud Run service.
The account and its IAM policies are managed in Terraform.
EOT
}

resource "google_project_iam_custom_role" "airflow_executor" {
  role_id     = "composer.executor"
  title       = "Cloud Composer Airflow Executor"
  description = "Custom role for Airflow executor to trigger DAGs"
  permissions = [
    "composer.environments.get",
    "composer.environments.executeAirflowCommand",
  ]
  project = var.project_id
}

# Cloud Scheduler invokes Cloud Run jobs with container overrides (SCHEDULE_ID
# env var); that path needs `run.jobs.runWithOverrides`, which is NOT in the
# built-in roles/run.invoker. roles/run.developer would grant it but also lets
# the SA modify Cloud Run resources — too broad. So we use a tightly scoped
# custom role.
resource "google_project_iam_custom_role" "run_job_invoker_with_overrides" {
  role_id     = "run.jobInvokerWithOverrides"
  title       = "Cloud Run Job Invoker With Overrides"
  description = "Adds run.jobs.runWithOverrides on top of roles/run.invoker, needed to invoke Cloud Run jobs with container overrides."
  permissions = [
    "run.jobs.runWithOverrides",
  ]
  project = var.project_id
}

resource "google_project_iam_member" "trigger_dag_iam" {
  for_each = toset([
    "projects/${var.project_id}/roles/composer.executor",
    "projects/${var.project_id}/roles/run.jobInvokerWithOverrides",
    "roles/run.invoker"
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.trigger_dag_cloud_run.email}"
  depends_on = [
    google_project_iam_custom_role.airflow_executor,
    google_project_iam_custom_role.run_job_invoker_with_overrides,
  ]
}

resource "google_storage_bucket_object" "trigger_dag_script" {
  name           = "dag-triggering/trigger_dag.sh"
  bucket         = var.source_files_bucket_name
  source         = "${path.module}/trigger_dag.sh"
  source_md5hash = filemd5("${path.module}/trigger_dag.sh")
}

resource "google_storage_bucket_iam_member" "trigger_dag_script_reader" {
  bucket = var.source_files_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.trigger_dag_cloud_run.email}"
}

resource "google_cloud_scheduler_job" "trigger_dag" {
  for_each         = local.dag_schedules_flat
  name             = "trigger-airflow-${each.key}"
  description      = "Triggers ${each.key} at ${each.value["schedule"]}"
  schedule         = each.value["schedule"]
  time_zone        = var.time_zone
  attempt_deadline = "320s"
  region           = "us-central1"
  project          = var.project_id

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri = format(
      "https://run.googleapis.com/v2/projects/%s/locations/%s/jobs/%s:run",
      var.project_id,
      var.composer.location,
      google_cloud_run_v2_job.trigger_dag[each.value["dag_name"]].name,
    )
    body = base64encode(jsonencode({
      overrides = {
        containerOverrides = [{
          # Set an env variable with the schedule ID that will be available inside
          # the Cloud Run job environment.
          env = [{ name = "SCHEDULE_ID", value = each.value.schedule_id }]
        }]
      }
    }))
    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.trigger_dag_cloud_run.email
    }
  }
}

locals {
  dag_schedules_flat = {
    for pair in flatten([
      for dag_name, dag_schedule_config in var.dags : [
        for schedule_id, cron in dag_schedule_config.schedule : {
          key         = "${dag_name}_${schedule_id}"
          dag_name    = dag_name
          schedule    = cron
          schedule_id = schedule_id
        }
      ]
      ]) : pair.key => {
      dag_name    = pair.dag_name
      schedule    = pair.schedule
      schedule_id = pair.schedule_id
    }
  }
}

resource "google_cloud_run_v2_job" "trigger_dag" {
  for_each = var.dags
  name     = replace("trigger-${each.key}", "_", "-")
  location = var.composer.location

  template {
    template {
      service_account = google_service_account.trigger_dag_cloud_run.email

      volumes {
        name = "source_files"
        gcs {
          bucket    = var.source_files_bucket_name
          read_only = true
        }
      }

      containers {
        image = "google/cloud-sdk:stable"

        volume_mounts {
          name       = "source_files"
          mount_path = "/source_files"
        }

        command = ["sh"]
        args = [
          "/source_files/dag-triggering/trigger_dag.sh",
          "--project", var.project_id,
          "--composer-env", var.composer.environment,
          "--composer-location", var.composer.location,
          "--dag-id", "${var.project_id}_${each.key}",
          "--config", jsonencode(coalesce(each.value.config, {})),
        ]

        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

