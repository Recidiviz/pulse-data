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

variable "project_id" {
  type        = string
  description = "GCP project ID where resources will be created"
}

variable "region" {
  type        = string
  description = "GCP region for Cloud Run service"
  default     = "us-central1"
}

variable "service_name" {
  type        = string
  description = "Name of the Cloud Run service"
  default     = "pagerduty-alert-forwarder"
}

variable "docker_image" {
  type        = string
  description = "Docker image for the alert forwarder service (defaults to Artifact Registry with :latest tag)"
  default     = ""
}

variable "pagerduty_services" {
  type        = list(string)
  description = "List of PagerDuty service names (must exist in PagerDuty)"
}

variable "cpu" {
  type        = string
  description = "CPU allocation for Cloud Run service"
  default     = "1000m"
}

variable "memory" {
  type        = string
  description = "Memory allocation for Cloud Run service"
  default     = "512Mi"
}

variable "max_instances" {
  type        = number
  description = "Maximum number of Cloud Run instances"
  default     = 4
}

variable "min_instances" {
  type        = number
  description = "Minimum number of Cloud Run instances (0 = scale to zero)"
  default     = 1
}

variable "timeout" {
  type        = string
  description = "Request timeout for Cloud Run service"
  default     = "60s"
}

variable "dlq_max_retry_attempts" {
  type        = number
  description = "Maximum retry attempts before sending to dead letter queue"
  default     = 5
}

variable "alert_notification_email" {
  type        = string
  description = "Email address (prefix) for dead letter queue alerts"
  default     = ""
}
