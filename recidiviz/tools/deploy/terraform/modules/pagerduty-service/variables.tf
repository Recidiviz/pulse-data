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

// The project id associated with the buckets and service accounts (ex: "recidiviz-123").
variable "project_id" {
  type = string
}

// The base name for this service. Used as a title for this service in the service directory page. Will be modified
// to include the information about the project.
variable "service_base_name" {
  type = string
}

// The description for this service.
variable "service_description" {
  type = string
}

// The ID for the escalation policy for incidents triggered for this service (in the form PXXXXXX).
// The list of available policies can be viewed at https://recidiviz.pagerduty.com/escalation_policies.
variable "escalation_policy_id" {
  type = string
}

// The base part of the username of the integration email for this service (the full username for this service's
// integration email will append`-{project_id}` to the base username within the monitoring DAG itself). Mail sent by our
// Sendgrid email accounts to this email can be used to trigger/resolve alerts.
variable "integration_email_base_username" {
  type = string
}

// Whether or not this is a service that produces PagerDuty alerts itself.
variable "is_monitoring_service" {
  type = bool
  default = false
}
