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

# The state code associated with the buckets and service accounts (ex: "TN").
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

# Name of the CloudSQL instance where new dbs will be provisioned.
variable "cloudsql_instance_name" {
  type = string
}

# Instance ID of the CloudSQL instance i.e. `recidiviz-staging:us-east1:dev-state-data`.
variable "cloudsql_instance_id" {
  type = string
}

# Region of the CloudSQL instance, e.g. "us-east1".
variable "cloudsql_instance_region" {
  type = string
}

# User name that should be used to log into the CloudSQL instance.
variable "cloudsql_instance_user_name" {
  type = string
}

# Password for cloudsql_instance_user_name.
variable "cloudsql_instance_user_password" {
  type = string
}

# PG v13 upgrade DB - Name of the CloudSQL instance where new dbs will be provisioned.
variable "v2_cloudsql_instance_name" {
  type = string
}

# PG v13 upgrade DB - Instance ID of the CloudSQL instance i.e. `recidiviz-staging:us-east1:dev-state-data`.
variable "v2_cloudsql_instance_id" {
  type = string
}

# PG v13 upgrade DB - Region of the CloudSQL instance, e.g. "us-east1".
variable "v2_cloudsql_instance_region" {
  type = string
}

# PG v13 upgrade DB - User name that should be used to log into the CloudSQL instance.
variable "v2_cloudsql_instance_user_name" {
  type = string
}

# PG v13 upgrade DB - Password for cloudsql_instance_user_name.
variable "v2_cloudsql_instance_user_password" {
  type = string
}

variable "repo_url" {
  type = string
}

locals {
  lower_state_code            = replace(lower(var.state_code), "_", "-")
  direct_ingest_formatted_str = "direct-ingest-state-${local.lower_state_code}"
}
