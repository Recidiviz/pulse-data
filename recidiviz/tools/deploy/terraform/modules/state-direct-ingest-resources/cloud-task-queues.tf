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


locals {
  raw_data_import_max_concurrent_dispatches = 5
  materialize_ingest_view_max_concurrent_dispatches = 5
}

module "scheduler-queue" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-scheduler"
  region                    = var.region
  max_dispatches_per_second = 100
}

module "scheduler-queue-secondary" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-scheduler-secondary"
  region                    = var.region
  max_dispatches_per_second = 100
}

module "raw-data-import-queue" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-raw-data-import"
  region                    = var.region
  max_concurrent_dispatches = local.raw_data_import_max_concurrent_dispatches
  max_dispatches_per_second = 100
}

module "raw-data-import-queue-secondary" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-raw-data-import-secondary"
  region                    = var.region
  max_concurrent_dispatches = local.raw_data_import_max_concurrent_dispatches
  max_dispatches_per_second = 100
}

module "materialize-ingest-view-queue" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-materialize-ingest-view"
  region                    = var.region
  max_concurrent_dispatches = local.materialize_ingest_view_max_concurrent_dispatches
  max_dispatches_per_second = 100
}

module "materialize-ingest-view-queue-secondary" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-materialize-ingest-view-secondary"
  region                    = var.region
  max_concurrent_dispatches = local.materialize_ingest_view_max_concurrent_dispatches
  max_dispatches_per_second = 100
}

module "extract-and-merge-queue" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-extract-and-merge"
  region                    = var.region
  max_dispatches_per_second = 100
}

module "extract-and-merge-queue-secondary" {
  source = "../base-task-queue"

  queue_name                = "${local.direct_ingest_formatted_str}-extract-and-merge-secondary"
  region                    = var.region
  max_dispatches_per_second = 100
}
