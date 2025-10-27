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

# Secret Manager secret for PagerDuty integration keys (JSON blob)
# Integration keys are automatically populated from PagerDuty provider

resource "google_secret_manager_secret" "pagerduty_integration_keys_json" {
  secret_id = "pagerduty-alert-forwarder-integration-keys"
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
      replicas {
        location = "us-east1"
      }
    }
  }

  labels = {
    purpose = "pagerduty-integration"
  }
}

# Store PagerDuty integration keys as JSON blob in Secret Manager
resource "google_secret_manager_secret_version" "pagerduty_integration_keys_json" {
  secret = google_secret_manager_secret.pagerduty_integration_keys_json.id
  secret_data = jsonencode({
    for service in var.pagerduty_services :
    service => pagerduty_service_integration.events_v2[service].integration_key
  })
}
