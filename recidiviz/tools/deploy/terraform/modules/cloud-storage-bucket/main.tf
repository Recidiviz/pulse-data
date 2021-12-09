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
terraform {
  experiments = [module_variable_optional_attrs]
}

# The project id associated with the buckets and service accounts (ex: "recidiviz-123").
variable "project_id" {
  type = string
}

# Combined with the project id to create the name (ex: "dashboard-data" becomes "recidiviz-123-dashboard-data")
variable "name_suffix" {
  type = string
}

# The location for the bucket, can be regional or multiregion (ex: "us-east1" or "us").
variable "location" {
  type    = string
  default = "us"
}

# The storage class of the bucket.
variable "storage_class" {
  type    = string
  default = "STANDARD"
}

# When true, access control to the bucket is controlled by IAM rather than
# object-level ACLs.
variable "uniform_bucket_level_access" {
  type    = bool
  default = true
}

# See https://cloud.google.com/storage/docs/lifecycle
variable "lifecycle_rules" {
  type = list(object({
    action = object({
      type = string
    }),
    condition = object({
      age                = optional(number)
      num_newer_versions = optional(number)
    })
  }))
  default = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 3
      }
    }
  ]
}

resource "google_storage_bucket" "bucket" {
  name                        = "${var.project_id}-${var.name_suffix}"
  location                    = var.location
  storage_class               = var.storage_class
  uniform_bucket_level_access = var.uniform_bucket_level_access

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type = lifecycle_rule.value["action"].type
      }
      condition {
        age                = lifecycle_rule.value["condition"].age
        num_newer_versions = lifecycle_rule.value["condition"].num_newer_versions
      }
    }
  }

  versioning {
    enabled = true
  }
}
