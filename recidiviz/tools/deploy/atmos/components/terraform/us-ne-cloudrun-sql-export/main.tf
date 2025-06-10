resource "google_cloud_run_v2_job" "us_ne_export_sql_server_raw_data_to_gcs" {
  name     = "us-ne-export-sql-server-raw-data-to-gcs"
  location = var.region
  project  = var.project_id
  provider = google-beta

  template {
    template {
      service_account = var.service_account_email
      timeout         = "3600s"
      max_retries     = 0
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:latest"
        command = ["pipenv"]
        args    = ["run", "python", "-m", "recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs", "--destination-bucket", var.ingest_bucket_name, "--dry-run", "True"]
        resources {
          limits = {
            # Need to use 4 CPUs if we want to have > 8Gi memory.
            # https://cloud.google.com/run/docs/configuring/services/memory-limits#cpu-minimum
            cpu    = "4000m"
            memory = "12Gi"
          }
        }
      }
      vpc_access {
        network_interfaces {
          network    = var.vpc_network
          subnetwork = var.vpc_subnetwork
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

resource "google_cloud_scheduler_job" "us_ne_trigger_sql_server_export_job" {
  name      = "us-ne-trigger-sql-server-export-job"
  project   = var.project_id
  schedule  = var.run_schedule
  region    = var.region
  time_zone = "UTC"

  http_target {
    uri         = "https://${google_cloud_run_v2_job.us_ne_export_sql_server_raw_data_to_gcs.location}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.us_ne_export_sql_server_raw_data_to_gcs.name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = var.service_account_email
    }

    headers = {
      "Content-Type" = "application/json"
    }
  }
}

resource "google_project_iam_member" "cloud_run_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "cloud_scheduler_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${var.service_account_email}"
}
