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

variable "region" {
  type    = string
  default = "us-central1"
}

# The region our app engine app resides
variable "app_engine_region" {
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

variable "github_username" {
  type        = string
  default     = "Recidiviz"
  description = "GitHub user to mirror the pulse-data repo from."
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

locals {
  repo_url      = "https://source.developers.google.com/projects/${var.project_id}/repos/github_${var.github_username}_pulse-data/revisions/${var.git_hash}/paths/recidiviz/cloud_functions"
  is_production = var.project_id == "recidiviz-123"
}
