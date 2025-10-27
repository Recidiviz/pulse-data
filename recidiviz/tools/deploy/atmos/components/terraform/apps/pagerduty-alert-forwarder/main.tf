# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
    google-beta = {
      source = "hashicorp/google-beta"
    }
    pagerduty = {
      source = "pagerduty/pagerduty"
    }
  }
}

data "google_secret_manager_secret" "pagerduty_token" {
  secret_id = "pagerduty_terraform_key"
}

data "google_secret_manager_secret_version" "pagerduty_token" {
  secret  = data.google_secret_manager_secret.pagerduty_token.name
  version = "latest"
}

locals {
  pagerduty_token = data.google_secret_manager_secret_version.pagerduty_token.secret_data
}

provider "pagerduty" {
  # The authentication token that allows us to connect to PagerDuty. See:
  # https://registry.terraform.io/providers/PagerDuty/pagerduty/latest/docs#token
  token = local.pagerduty_token
}

# Local variables
locals {
  # Default docker image path if not explicitly provided
  default_docker_image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.alert_forwarder.repository_id}/pagerduty-alert-forwarder:latest"
  docker_image_tag     = var.docker_image != "" ? var.docker_image : local.default_docker_image
}

# GCS bucket for configuration files
resource "google_storage_bucket" "config" {
  name     = "${var.project_id}-pagerduty-alert-forwarder-config"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    purpose = "pagerduty-alert-forwarder-config"
  }
}

# Upload config.yaml to GCS bucket
resource "google_storage_bucket_object" "config" {
  name   = "config.yaml"
  bucket = google_storage_bucket.config.name
  source = "${path.module}/../../../../../../../monitoring/pagerduty_alert_forwarder/config.yaml"

  # Force update on content change
  detect_md5hash = "different"
}

# Artifact Registry repository for Docker images
resource "google_artifact_registry_repository" "alert_forwarder" {
  location      = var.region
  repository_id = "pagerduty-alert-forwarder"
  description   = "Docker images for PagerDuty alert forwarder service"
  format        = "DOCKER"
  project       = var.project_id

  labels = {
    purpose = "alert-forwarding"
  }
}

# Fetch the Docker image to get its digest for automatic redeployment
data "google_artifact_registry_docker_image" "alert_forwarder" {
  location      = var.region
  repository_id = google_artifact_registry_repository.alert_forwarder.repository_id
  image_name    = "pagerduty-alert-forwarder:latest"
  project       = var.project_id
}

# Compute the final docker image reference with digest
locals {
  # Use digest from data source if available, otherwise fall back to tag
  docker_image = try(
    data.google_artifact_registry_docker_image.alert_forwarder.self_link,
    local.docker_image_tag
  )
}

# Service account for Cloud Run service
resource "google_service_account" "alert_forwarder" {
  account_id   = "${var.service_name}-sa"
  display_name = "Alert Forwarder Service Account"
  description  = "Service account for alert forwarder Cloud Run service"
  project      = var.project_id
}

# Cloud Run service
resource "google_cloud_run_v2_service" "alert_forwarder" {
  name     = var.service_name
  location = var.region
  project  = var.project_id
  provider = google-beta
  deletion_protection = false

  template {
    service_account = google_service_account.alert_forwarder.email

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    timeout = var.timeout

    # Volume for mounting GCS bucket with config
    volumes {
      name = "config"
      gcs {
        bucket    = google_storage_bucket.config.name
        read_only = true
      }
    }

    containers {
      image = local.docker_image

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
      }

      # Mount config volume
      volume_mounts {
        name       = "config"
        mount_path = "/app/config"
      }

      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }

      env {
        name  = "PAGERDUTY_SERVICES"
        value = join(",", var.pagerduty_services)
      }

      env {
        name  = "CONFIG_PATH"
        value = "/app/config/config.yaml"
      }

      # Mount PagerDuty integration keys as JSON blob
      env {
        name = "PAGERDUTY_INTEGRATION_KEYS"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.pagerduty_integration_keys_json.secret_id
            version = "latest"
          }
        }
      }

      # Health check on /health endpoint
      liveness_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 10
        period_seconds        = 30
        timeout_seconds       = 5
        failure_threshold     = 3
      }

      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 0
        period_seconds        = 10
        timeout_seconds       = 5
        failure_threshold     = 3
      }
    }
  }

  lifecycle {
    ignore_changes = []
    prevent_destroy = false
  }
}

# IAM: Allow unauthenticated access from Pub/Sub
# Pub/Sub will authenticate using OIDC token with service account
resource "google_cloud_run_v2_service_iam_member" "pubsub_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.alert_forwarder.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.alert_forwarder.email}"
}

# Pub/Sub topic for Cloud Monitoring alerts
resource "google_pubsub_topic" "monitoring_alerts" {
  name    = "cloud-monitoring-alerts-to-forward"
  project = var.project_id

  labels = {
    purpose = "alert-forwarding"
  }
}

# Dead letter queue topic
resource "google_pubsub_topic" "dead_letter_queue" {
  name    = "pagerduty-alert-forwarder-dlq"
  project = var.project_id

  labels = {
    purpose = "dead-letter-queue"
  }
}

# Pub/Sub subscription with push to Cloud Run
resource "google_pubsub_subscription" "alert_forwarder" {
  name    = "pagerduty-alert-forwarder-subscription"
  topic   = google_pubsub_topic.monitoring_alerts.name
  project = var.project_id

  # Push configuration
  push_config {
    push_endpoint = "${google_cloud_run_v2_service.alert_forwarder.uri}/pubsub-push"

    oidc_token {
      service_account_email = google_service_account.alert_forwarder.email
    }
  }

  # Acknowledgment deadline
  ack_deadline_seconds = 60

  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Dead letter policy
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter_queue.id
    max_delivery_attempts = var.dlq_max_retry_attempts
  }

  # Message retention
  message_retention_duration = "604800s" # 7 days

  # Never expire
  expiration_policy {
    ttl = ""
  }

  depends_on = [
    google_pubsub_topic.dead_letter_queue,
    google_pubsub_topic_iam_member.dlq_publisher,
  ]
}

# DLQ subscription for monitoring
resource "google_pubsub_subscription" "dlq_pull" {
  name    = "pagerduty-alert-forwarder-dlq-pull"
  topic   = google_pubsub_topic.dead_letter_queue.name
  project = var.project_id

  # Pull subscription for manual inspection
  ack_deadline_seconds = 600

  # Retain messages for 7 days
  message_retention_duration = "604800s"

  # Never expire
  expiration_policy {
    ttl = ""
  }
}

# IAM: Allow subscription to publish to DLQ
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter_queue.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# IAM: Allow subscription to subscribe to DLQ
resource "google_pubsub_subscription_iam_member" "dlq_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.alert_forwarder.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Get project number for Pub/Sub service account
data "google_project" "project" {
  project_id = var.project_id
}

# IAM: Allow Cloud Monitoring to publish to alert topic
resource "google_pubsub_topic_iam_member" "monitoring_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.monitoring_alerts.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-monitoring-notification.iam.gserviceaccount.com"
}

# IAM: Secret Manager access for PagerDuty keys
resource "google_project_iam_member" "secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.alert_forwarder.email}"
}

# IAM: Logging
resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.alert_forwarder.email}"
}

# IAM: Artifact Registry access for pulling Docker images
resource "google_artifact_registry_repository_iam_member" "reader" {
  project    = var.project_id
  location   = google_artifact_registry_repository.alert_forwarder.location
  repository = google_artifact_registry_repository.alert_forwarder.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.alert_forwarder.email}"
}

# IAM: GCS bucket access for reading config files
resource "google_storage_bucket_iam_member" "config_reader" {
  bucket = google_storage_bucket.config.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.alert_forwarder.email}"
}

# Monitoring alert for DLQ depth
resource "google_monitoring_alert_policy" "dlq_depth" {
  display_name = "Alert Forwarder - Dead Letter Queue Depth"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "DLQ has unprocessed messages"

    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND resource.labels.subscription_id = \"${google_pubsub_subscription.dlq_pull.name}\" AND metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  notification_channels = var.alert_notification_email != "" ? [
    google_monitoring_notification_channel.email[0].id
  ] : []

  alert_strategy {
    auto_close = "86400s" # 24 hours
  }

  documentation {
    content = <<-EOT
      The alert forwarder dead letter queue has unprocessed messages.

      This indicates that the alert forwarder service failed to process some Cloud Monitoring alerts
      after ${var.dlq_max_retry_attempts} retry attempts.

      **Action Required:**
      1. Check Cloud Run logs for the pagerduty-alert-forwarder service for errors
      2. Verify PagerDuty integration keys are valid
      3. Review messages in the DLQ: `gcloud pubsub subscriptions pull ${google_pubsub_subscription.dlq_pull.name} --limit=10 --project ${var.project_id}`
      4. Fix the underlying issue
      5. Optionally re-process DLQ messages manually

      If there is no need for this event to be processed, purge the queue:
      ```
      gcloud pubsub subscriptions pull ${google_pubsub_subscription.dlq_pull.name} \
        --auto-ack \
        --limit=10 \
        --project=${var.project_id}
      ```
    EOT
    mime_type = "text/markdown"
  }
}

# Email notification channel for DLQ alerts
resource "google_monitoring_notification_channel" "email" {
  count        = var.alert_notification_email != "" ? 1 : 0
  display_name = "Alert Forwarder DLQ Email"
  project      = var.project_id
  type         = "email"

  labels = {
    email_address = format("%s@recidiviz.org", var.alert_notification_email)
  }
}

# Cloud Monitoring notification channel for forwarding alerts to Pub/Sub
resource "google_monitoring_notification_channel" "pubsub" {
  display_name = "PagerDuty Alert Forwarder"
  project      = var.project_id
  type         = "pubsub"

  labels = {
    topic = google_pubsub_topic.monitoring_alerts.id
  }

  description = "Routes Cloud Monitoring alerts to PagerDuty via alert forwarder service"
}

# Data source to lookup PagerDuty services by name
data "pagerduty_service" "services" {
  for_each = toset(var.pagerduty_services)
  name     = each.key
}

# Create Events API V2 integrations for each PagerDuty service
resource "pagerduty_service_integration" "events_v2" {
  for_each = toset(var.pagerduty_services)
  name     = "Cloud Monitoring Alert Forwarder"
  service  = data.pagerduty_service.services[each.key].id
  type     = "events_api_v2_inbound_integration"
}
