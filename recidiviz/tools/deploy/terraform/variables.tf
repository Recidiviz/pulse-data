# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
  type = string
}

variable "registry_project_id" {
  type        = string
  default     = "recidiviz-staging"
  description = "Project ID to source container images from. Defaults to recidiviz-staging."
}

variable "us_central_region" {
  type    = string
  default = "us-central1"
}

variable "us_east_region" {
  type    = string
  default = "us-east1"
}

# To the extent possible, we keep all direct-ingest buckets in us-east1.
# See #5253 for more context.
variable "direct_ingest_region" {
  type    = string
  default = "us-east1"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "git_hash" {
  type = string
}

variable "docker_image_tag" {
  type = string
}

variable "max_case_triage_instances" {
  type = number
  # Note: if we adjust this instance number upward, we may have to adjust
  # the number of max connections in our postgres instances.
  # See the dicussion in #5497 for more context, and see the docs:
  # https://cloud.google.com/sql/docs/quotas#postgresql for more.
  default = 3
}

variable "max_application_import_instances" {
  type = number
  # The Cloud Run default is 100, and if we had a bug causing us to scale that high that
  # could be a costly error.
  # 5 instances at 2 concurrent requests per container means a maximum of 10 in-flight
  # requests can be served at once
  default = 5
}

variable "max_asset_generation_instances" {
  type = number
  # Start with 3 as the default to match our other services. The Cloud Run
  # default is 100, and if we had a bug causing us to scale that high that
  # could be a costly error.
  default = 3
}

variable "github_username" {
  type        = string
  default     = "Recidiviz"
  description = "GitHub user to mirror the pulse-data repo from."
}

# The authentication token that allows us to connect to PagerDuty. See:
# https://registry.terraform.io/providers/PagerDuty/pagerduty/latest/docs#token
variable "pagerduty_token" {
  type      = string
  sensitive = true
}

variable "direct_ingest_state_storage_secondary_bucket_name_suffix" {
  type        = string
  default     = "direct-ingest-state-storage-secondary"
  description = <<EOT
Name suffix of the direct ingest state storage secondary bucket.
Used when deploying to a project that causes the full bucket name to be >63 characters
(see https://cloud.google.com/storage/docs/naming-buckets#requirements).
This affects projects whose names are >=26 characters.
Defaults to direct-ingest-state-storage-secondary.
EOT
}

variable "default_sql_tier" {
  type        = string
  default     = ""
  description = <<EOT
Default tier to apply to Cloud SQL instances, to enable dev projects to use custom values.
This is useed in cases like the terraform sandbox project, where we want all databases to be as
small as possible.
EOT
}

locals {
  is_production  = var.project_id == "recidiviz-123"
  recidiviz_root = "${path.root}/../../.."
}
