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
module "justice-counts-data-bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "justice-counts-data"
}

module "direct-ingest-state-storage" {
  source = "./modules/cloud-storage-bucket"

  project_id    = var.project_id
  location      = var.direct_ingest_region
  storage_class = "REGIONAL"
  name_suffix   = "direct-ingest-state-storage"
}

module "direct-ingest-state-storage-secondary" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  location    = var.direct_ingest_region
  name_suffix = var.direct_ingest_state_storage_secondary_bucket_name_suffix
}

module "direct-ingest-cloud-sql-exports" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "cloud-sql-exports"
}

module "dashboard-user-restrictions-bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "dashboard-user-restrictions"
}

module "state-aggregate-reports" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "state-aggregate-reports"
}
