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

output "service_url" {
  description = "URL of the Cloud Run service"
  value       = google_cloud_run_v2_service.alert_forwarder.uri
}

output "service_name" {
  description = "Name of the Cloud Run service"
  value       = google_cloud_run_v2_service.alert_forwarder.name
}

output "pubsub_topic_name" {
  description = "Name of the Pub/Sub topic for Cloud Monitoring alerts"
  value       = google_pubsub_topic.monitoring_alerts.name
}

output "pubsub_topic_id" {
  description = "Full resource ID of the Pub/Sub topic"
  value       = google_pubsub_topic.monitoring_alerts.id
}

output "service_account_email" {
  description = "Email of the service account used by the Cloud Run service"
  value       = google_service_account.alert_forwarder.email
}

output "dead_letter_topic_name" {
  description = "Name of the dead letter queue topic"
  value       = google_pubsub_topic.dead_letter_queue.name
}

output "pagerduty_integration_ids" {
  description = "Map of PagerDuty service names to integration IDs"
  value = {
    for service, integration in pagerduty_service_integration.events_v2 :
    service => integration.id
  }
}

output "pagerduty_secret_name" {
  description = "Name of the Secret Manager secret containing PagerDuty integration keys JSON"
  value       = google_secret_manager_secret.pagerduty_integration_keys_json.secret_id
}

output "artifact_registry_repository" {
  description = "Name of the Artifact Registry repository"
  value       = google_artifact_registry_repository.alert_forwarder.name
}

output "artifact_registry_repository_url" {
  description = "Full URL of the Artifact Registry repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.alert_forwarder.repository_id}"
}

output "docker_image" {
  description = "Docker image being used by Cloud Run"
  value       = local.docker_image
}

output "notification_channel_id" {
  description = "ID of the Cloud Monitoring notification channel for PagerDuty forwarding"
  value       = google_monitoring_notification_channel.pubsub.id
}

output "notification_channel_name" {
  description = "Name of the Cloud Monitoring notification channel for PagerDuty forwarding"
  value       = google_monitoring_notification_channel.pubsub.name
}