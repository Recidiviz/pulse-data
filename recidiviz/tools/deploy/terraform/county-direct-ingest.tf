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

module "county_direct_ingest_buckets" {
  for_each = toset(keys(local.direct_ingest_county_manifests))
  source   = "./modules/county-direct-ingest-resources"

  county_code = each.key
  region      = "us-east1"
  project_id  = var.project_id
}

module "direct_ingest_queues" {
  for_each = toset([
    "direct-ingest-scheduler-v2",        # TODO(#6455) Rename to "direct-ingest-jpp-scheduler-v2"
    "direct-ingest-bq-import-export-v2", # TODO(#6455) Rename to "direct-ingest-jpp-bq-import-export-v2"
    "direct-ingest-jpp-process-job-queue-v2",
  ])

  source = "./modules/serial-task-queue"

  queue_name                = each.key
  region                    = var.app_engine_region
  max_dispatches_per_second = 100
}

module "direct-ingest-county-storage" {
  source = "./modules/cloud-storage-bucket"

  project_id    = var.project_id
  location      = var.direct_ingest_region
  storage_class = "REGIONAL"
  name_suffix   = "direct-ingest-county-storage"

  labels = {
    recidiviz_service = "scrapers"
  }
}
