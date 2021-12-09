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

module "sftp-storage-bucket" {
  source = "../cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = local.direct_ingest_sftp_str
  location    = var.region

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 14
      }
    }
  ]
}

module "sftp-download-queue" {
  source = "../serial-task-queue"

  queue_name                = "${local.direct_ingest_sftp_str}-queue"
  region                    = var.region
  max_dispatches_per_second = 5
  max_retry_attempts        = 5
  logging_sampling_ratio    = 1.0
}
