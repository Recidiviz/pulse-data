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

# TODO(#6052): Refactor to use ../cloud-storage-bucket
resource "google_storage_bucket" "direct-ingest-bucket" {
  name                        = "${var.project_id}-${local.direct_ingest_formatted_str}"
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 3
    }
  }

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "prod-only-testing-direct-ingest-bucket" {
  count                       = var.is_production ? 1 : 0
  name                        = "recidiviz-123-${local.direct_ingest_formatted_str}-upload-testing"
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 3
    }
  }

  versioning {
    enabled = true
  }
}

module "secondary-direct-ingest-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "${local.direct_ingest_formatted_str}-secondary"
  location    = var.region
}
