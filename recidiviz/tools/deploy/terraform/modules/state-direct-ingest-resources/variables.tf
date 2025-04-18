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

# The state code associated with the buckets and service accounts (ex: "US_TN").
variable "state_code" {
  type = string
}

# the project id associated with the buckets and service accounts (ex: "recidiviz-123").
variable "project_id" {
  type = string
}

# The preferred region for the instance (ex: "us-east4").
variable "region" {
  type = string
}

# Whether or not the project is in production.
variable "is_production" {
  type = bool
}

# The role to associate with the service accounts.
variable "state_admin_role" {
  type = string
}

# Object containing info from this state's manifest.yaml
variable "region_manifest" {
  type = object({
    environment = string
  })
}

variable "raw_data_storage_notification_topic_id" {
  type = string
}

locals {
  lower_state_code            = replace(lower(var.state_code), "_", "-")
  direct_ingest_formatted_str = "direct-ingest-state-${local.lower_state_code}"
  is_ingest_launched          = (!var.is_production || var.region_manifest.environment == "production")
}
