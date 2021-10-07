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

resource "google_cloud_tasks_queue" "admin_panel_data_discovery_queue" {
  name     = "admin-panel-data-discovery"
  location = var.app_engine_region

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

resource "google_cloud_tasks_queue" "scraper_phase_queue" {
  name     = "scraper-phase-v2"
  location = var.app_engine_region

  rate_limits {
    max_dispatches_per_second = 1
    max_concurrent_dispatches = 100
  }

  retry_config {
    max_attempts = 5
    min_backoff  = "5s"
    max_backoff  = "300s"
  }

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

resource "google_cloud_tasks_queue" "job_monitor_queue" {
  name     = "job-monitor-v2"
  location = var.app_engine_region

  rate_limits {
    max_dispatches_per_second = 25
    max_concurrent_dispatches = 25
  }

  retry_config {
    max_attempts = 5
    max_backoff  = "120s"
    min_backoff  = "5s"
  }

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

module "bigquery-queue" {
  source = "./modules/serial-task-queue"

  queue_name         = "bigquery-v2"
  region             = var.app_engine_region
  max_retry_attempts = 1
}

module "case-triage-db-operations-queue" {
  source = "./modules/serial-task-queue"

  queue_name                = "case-triage-db-operations-queue"
  region                    = var.app_engine_region
  max_dispatches_per_second = 100
}

locals {
  # Region Queues
  ingest_scrape_manifest = fileset("${local.recidiviz_root}/ingest/scrape/regions", "*/manifest.yaml")
  region_manifests = merge(
    # Seeing "too many open files" errors? Try running `ulimit -n 1024`
    { for f in local.ingest_scrape_manifest : dirname(f) => yamldecode(file("${local.recidiviz_root}/ingest/scrape/regions/${f}")) },
    { for region_code_upper, manifest in local.direct_ingest_region_manifests : lower(region_code_upper) => manifest }
  )
  region_queues = { for region, m in local.region_manifests : region => {
    # The below should be able to be try(m.queue.rate_limits.max_dispatches_per_second, null),
    # but that doesn't work: https://github.com/hashicorp/terraform/issues/24142
    max_dispatches_per_second = try(m.queue.rate_limits.max_dispatches_per_second, 0.083333333)
  } if try(m.shared_queue, null) == null && (!local.is_production || try(m.environment, null) == "production") }

  # Vendor Queues
  vendor_queue_files = fileset("${local.recidiviz_root}/ingest/scrape/vendors", "*/queue.yaml")
  vendor_manifests   = { for f in local.vendor_queue_files : dirname(f) => yamldecode(file("${local.recidiviz_root}/ingest/scrape/vendors/${f}")) }
  vendor_queues = { for vendor, m in local.vendor_manifests : vendor => {
    max_dispatches_per_second = try(m.rate_limits.max_dispatches_per_second, 0.083333333)
    max_concurrent_dispatches = try(m.rate_limits.max_concurrent_dispatches, 3)
  } }
}

module "scraper-region-queues" {
  for_each = local.region_queues

  source = "./modules/base-scraper-task-queue"

  queue_name                = "${replace(each.key, "_", "-")}-scraper-v2"
  region                    = var.app_engine_region
  max_dispatches_per_second = each.value.max_dispatches_per_second
}

module "scraper-vendor-queues" {
  for_each = local.vendor_queues

  source = "./modules/base-scraper-task-queue"

  queue_name                = "vendor-${replace(each.key, "_", "-")}-scraper-v2"
  region                    = var.app_engine_region
  max_dispatches_per_second = each.value.max_dispatches_per_second
  max_concurrent_dispatches = each.value.max_concurrent_dispatches
}
