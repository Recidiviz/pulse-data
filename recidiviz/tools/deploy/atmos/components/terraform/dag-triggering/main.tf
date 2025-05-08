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

resource "google_project_iam_member" "trigger_dag_iam" {
  for_each = toset([
    "projects/${var.project_id}/roles/composer.executor",
    "roles/run.invoker"
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.trigger_dag_cloud_run.email}"
  depends_on = [
  google_project_iam_custom_role.airflow_executor]
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
      "https://%s-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/%s/jobs/%s:run",
      var.composer.location,
      var.project_id,
      google_cloud_run_v2_job.trigger_dag[each.value["dag_name"]].name,
    )

    oauth_token {
      service_account_email = google_service_account.trigger_dag_cloud_run.email
    }
  }
}

locals {
  dag_schedules_flat = {
    for pair in flatten([
      for dag_name, dag_schedule_config in var.dags : [
        for idx, cron in dag_schedule_config.schedule : {
          key      = "${dag_name}_${idx}"
          dag_name = dag_name
          schedule = cron
        }
      ]
      ]) : pair.key => {
      dag_name = pair.dag_name
      schedule = pair.schedule
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

      containers {
        image   = "google/cloud-sdk:stable"
        command = ["gcloud"]
        args = [
          "composer", "environments",
          "run",
          var.composer.environment,
          "--location",
          var.composer.location,
          "dags",
          "trigger",
          "--",
          "${var.project_id}_${each.key}",
          "-c",
          jsonencode(coalesce(each.value.config, {})),
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

