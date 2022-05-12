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

resource "google_cloud_tasks_queue" "base_queue" {
  name     = var.queue_name
  location = var.region

  rate_limits {
    max_dispatches_per_second = var.max_dispatches_per_second
    max_concurrent_dispatches = var.max_concurrent_dispatches
  }

  retry_config {
    max_attempts = var.max_retry_attempts
  }

  stackdriver_logging_config {
    sampling_ratio = var.logging_sampling_ratio
  }
}

# This block tells TF that the resource that used to be called serial_queue is now
# named base_queue so that it doesn't delete and recreate the queues during the
# rename. This block can be removed once it has been deployed to prod. For more, see:
# https://www.terraform.io/language/modules/develop/refactoring.
moved {
  from = google_cloud_tasks_queue.serial_queue
  to = google_cloud_tasks_queue.base_queue
}
