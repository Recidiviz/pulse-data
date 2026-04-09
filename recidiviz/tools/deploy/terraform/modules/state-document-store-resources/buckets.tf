# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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

locals {
  lower_hyphened_state_code = replace(lower(var.state_code), "_", "-")
}

# bucket to store temporary task output results generated during document store process
module "document-store-upload-results-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "${local.lower_hyphened_state_code}-temp-document-store-output"
  location    = var.region

  lifecycle_rules = [{
    action = {
      type = "Delete"
    }
    condition = {
      age = 7
    }
  }]
}

# definitive storage for document blobs in a state
module "document-blob-storage-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "${local.lower_hyphened_state_code}-document-blob-storage"
  location    = var.region
}
