# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

# Create a new service account for Case Triage Cloud Run
resource "google_service_account" "cloud_run" {
  account_id   = "cloud-run-service-account"
  display_name = "Case Triage Cloud Run Service Account"
  description  = <<EOT
Service Account that acts as the identity for the Case Triage Cloud Run service.
The account and its IAM policies are managed in Terraform.
EOT
}

resource "google_service_account" "application_data_import_cloud_run" {
  account_id   = "application-data-import-cr"
  display_name = "Application Data Import Cloud Run Service Account"
  description  = <<EOT
Service Account that acts as the identity for the Application Data Import Cloud Run service.
The account and its IAM policies are managed in Terraform (see #13024).
EOT
}

locals {
  cloud_run_common_roles = [
    "roles/run.admin",
    "roles/secretmanager.secretAccessor",
    "roles/cloudsql.client",
    google_project_iam_custom_role.gcs-object-and-bucket-viewer.name,
    "roles/logging.logWriter"
  ]
  application_import_roles = concat(local.cloud_run_common_roles, [
    "roles/cloudtasks.enqueuer",
    # Use role_id to get a value known at plan-time so Terraform can calculate the length of
    # toset(application_import_roles) before the custom role has been created.
    "projects/${var.project_id}/roles/${google_project_iam_custom_role.sql-importer.role_id}"
  ])
}

moved {
  from = google_project_iam_member.cloud_run_secret_accessor
  to   = google_project_iam_member.case_triage_iam["roles/secretmanager.secretAccessor"]
}
moved {
  from = google_project_iam_member.cloud_run_admin
  to   = google_project_iam_member.case_triage_iam["roles/run.admin"]
}

moved {
  from = google_project_iam_member.cloud_run_cloud_sql
  to   = google_project_iam_member.case_triage_iam["roles/cloudsql.client"]
}

moved {
  from = google_project_iam_member.cloud_run_gcs_access
  to   = google_project_iam_member.case_triage_iam["projects/recidiviz-staging/roles/gcsObjectAndBucketViewer"]
}

moved {
  from = google_project_iam_member.cloud_run_log_writer
  to   = google_project_iam_member.case_triage_iam["roles/logging.logWriter"]
}

resource "google_project_iam_member" "case_triage_iam" {
  for_each = toset(local.cloud_run_common_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "application_data_import_iam" {
  for_each = toset(local.application_import_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.application_data_import_cloud_run.email}"
}

resource "google_service_account_iam_member" "application_data_import_iam" {
  service_account_id = google_service_account.application_data_import_cloud_run.name
  # Grant serviceAccountUser on itself. Without this, trying to create an OIDC token to give to
  # Cloud Tasks fails with the error "The principal (user or service account) lacks IAM permission
  # "iam.serviceAccounts.actAs" for the resource "application-data-import-cr@recidiviz-staging.iam.gserviceaccount.com"
  # (or the resource may not exist)."
  role   = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.application_data_import_cloud_run.email}"
}


# Use existing Justice Counts service account for Justice Counts Cloud Run
data "google_service_account" "justice_counts_cloud_run" {
  account_id = var.project_id == "recidiviz-123" ? "justice-counts-spotlights-prod" : "jstc-counts-spotlights-staging"
}

# Grab existing Justice Counts Cloud Run service to determine current revision
data "google_cloud_run_service" "existing_justice_counts_cloud_run" {
  name     = "justice-counts-web"
  location = var.region
}

# Env vars from secrets
data "google_secret_manager_secret_version" "segment_write_key" { secret = "case_triage_segment_backend_key" }

# Initializes Case Triage Cloud Run service
resource "google_cloud_run_service" "case-triage" {
  name     = "case-triage-web"
  location = var.region

  template {
    spec {
      containers {
        image   = "us.gcr.io/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.case_triage.server:app"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }

        env {
          name  = "APP_URL"
          value = var.project_id == "recidiviz-123" ? "https://app.recidiviz.org" : "https://app-staging.recidiviz.org"
        }

        env {
          name  = "DASHBOARD_URL"
          value = "https://dashboard.recidiviz.org"
        }

        env {
          name  = "SEGMENT_WRITE_KEY"
          value = data.google_secret_manager_secret_version.segment_write_key.secret_data
        }

        env {
          name  = "AUTH0_CLAIM_NAMESPACE"
          value = "https://dashboard.recidiviz.org"
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "1024Mi"
          }
        }
      }

      service_account_name = google_service_account.cloud_run.email
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"        = 1
        "autoscaling.knative.dev/maxScale"        = var.max_case_triage_instances
        "run.googleapis.com/cloudsql-instances"   = local.joined_connection_string
        "run.googleapis.com/vpc-access-connector" = google_vpc_access_connector.us_central_redis_vpc_connector.name
        "run.googleapis.com/vpc-access-egress"    = "private-ranges-only"
      }

      # If a terraform apply fails for a given deploy, we may retry again some time later after a fix has landed. When
      # we reattempt, the docker image tag (version number) will remain the same. If we only include the image tag but
      # not the hash in the name and the cloud run deploy succeeded during the first attempt, Terraform will not
      # recognize that we need to re-deploy the Cloud Run service on the second attempt, even if changes have landed
      # between attempts #1 and #2. For this reason, we instead include the git hash in the service name.
      name = "case-triage-web-${local.git_short_hash}"
    }
  }

  metadata {
    annotations = {
      "run.googleapis.com/ingress"        = "all"
      "run.googleapis.com/ingress-status" = "all"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = false
}

# Initializes Justice Counts Cloud Run service
resource "google_cloud_run_service" "justice-counts" {
  name     = "justice-counts-web"
  location = var.region

  template {
    spec {
      containers {
        image   = "us.gcr.io/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.justice_counts.control_panel.server:create_app()"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "1024Mi"
          }
        }
      }

      service_account_name = data.google_service_account.justice_counts_cloud_run.email
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"      = 1
        "autoscaling.knative.dev/maxScale"      = var.max_justice_counts_instances
        "run.googleapis.com/cloudsql-instances" = module.justice_counts_database.connection_name
      }

      # If a terraform apply fails for a given deploy, we may retry again some time later after a fix has landed. When
      # we reattempt, the docker image tag (version number) will remain the same. If we only include the image tag but
      # not the hash in the name and the cloud run deploy succeeded during the first attempt, Terraform will not
      # recognize that we need to re-deploy the Cloud Run service on the second attempt, even if changes have landed
      # between attempts #1 and #2. For this reason, we instead include the git hash in the service name.
      name = "justice-counts-web-${local.git_short_hash}"
    }
  }

  # Keep 100% of traffic on previous revision. This makes it possible for us 
  # to create and serve revisions in the UI without them being overridden by 
  # a subsequent data platform deploy.
  traffic {
    percent         = 100
    revision_name   = data.google_cloud_run_service.existing_justice_counts_cloud_run.template[0].metadata[0].name
  }

  traffic {
    percent         = 0
    latest_revision = true
  }

  autogenerate_revision_name = false
}

# Initializes Application Data Import Cloud Run service
resource "google_cloud_run_service" "application-data-import" {
  name     = "application-data-import"
  location = var.region

  template {
    spec {
      containers {
        image   = "us.gcr.io/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.application_data_import.server:app"]

        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "1024Mi"
          }
        }
      }

      service_account_name = google_service_account.application_data_import_cloud_run.email
    }

    metadata {
      annotations = {
        # No need for a min scale. We don't need fast start times for this, so we can have no
        # instances when it's not being used.
        "autoscaling.knative.dev/maxScale"        = var.max_application_import_instances
        "run.googleapis.com/cloudsql-instances"   = local.application_data_connection_string
        "run.googleapis.com/vpc-access-connector" = google_vpc_access_connector.us_central_redis_vpc_connector.name
        "run.googleapis.com/vpc-access-egress"    = "private-ranges-only"
      }

      # If a terraform apply fails for a given deploy, we may retry again some time later after a fix has landed. When
      # we reattempt, the docker image tag (version number) will remain the same. If we only include the image tag but
      # not the hash in the name and the cloud run deploy succeeded during the first attempt, Terraform will not
      # recognize that we need to re-deploy the Cloud Run service on the second attempt, even if changes have landed
      # between attempts #1 and #2. For this reason, we instead include the git hash in the service name.
      name = "application-data-import-${local.git_short_hash}"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = false
}

# By default, Cloud Run services are private and secured by IAM. 
# The blocks below set up public access so that anyone (e.g. our frontends)
# can invoke the services through an HTTP endpoint.
resource "google_cloud_run_service_iam_member" "public-access" {
  location = google_cloud_run_service.case-triage.location
  project  = google_cloud_run_service.case-triage.project
  service  = google_cloud_run_service.case-triage.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_service_iam_member" "justice-counts-public-access" {
  location = google_cloud_run_service.justice-counts.location
  project  = google_cloud_run_service.justice-counts.project
  service  = google_cloud_run_service.justice-counts.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Setting up load balancer
# Drawn from https://github.com/terraform-google-modules/terraform-google-lb-http/blob/master/examples/cloudrun/main.tf
resource "google_compute_region_network_endpoint_group" "serverless_neg" {
  provider              = google-beta
  name                  = "unified-product-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region
  cloud_run {
    service = google_cloud_run_service.case-triage.name
  }
}

resource "google_compute_ssl_policy" "modern-ssl-policy" {
  name            = "modern-ssl-policy"
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"
}

module "unified-product-load-balancer" {
  source  = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version = "~> 6.2.0"
  name    = "unified-product-lb"
  project = var.project_id

  ssl                             = true
  ssl_policy                      = google_compute_ssl_policy.modern-ssl-policy.name
  managed_ssl_certificate_domains = local.is_production ? ["app-prod.recidiviz.org", "app.recidiviz.org"] : ["app-staging.recidiviz.org"]
  https_redirect                  = true

  backends = {
    default = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.serverless_neg.id
        }
      ]
      enable_cdn              = true
      security_policy         = null
      custom_request_headers  = null
      custom_response_headers = null

      iap_config = {
        enable               = false
        oauth2_client_id     = ""
        oauth2_client_secret = ""
      }
      log_config = {
        enable      = true
        sample_rate = null
      }
    }
  }
}

locals {
  # Take a substring of the hash so we don't run into character limits on metadata.name,
  # which can be at most 63 characters. We don't necessarily need the whole hash there,
  # we just need to know if the code being pushed to Cloud Run has changed.
  git_short_hash = substr(var.git_hash, 0, 8)
}
