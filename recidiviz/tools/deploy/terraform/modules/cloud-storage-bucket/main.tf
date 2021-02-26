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

# The project id associated with the buckets and service accounts (ex: "recidiviz-123").
variable "project_id" {
  type = string
}

# Combined with the project id to creat the name (ex: "dashboard-data" becomes "recidiviz-123-dashboard-data")
variable "name_suffix" {
  type = string
}

# The location for the bucket, can be regional or multiregion (ex: "us-east1" or "us").
variable "location" {
  type    = string
  default = "us"
}

resource "google_storage_bucket" "bucket" {
  name                        = "${var.project_id}-${var.name_suffix}"
  location                    = var.location
  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 2
    }
  }

  versioning {
    enabled = true
  }
}
