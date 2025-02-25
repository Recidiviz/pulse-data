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

variable "queue_name" {
  type = string
}

variable "region" {
  type = string
}

// Maps to `max_concurrent_dispatches` in the `google_cloud_tasks_queue` resource.
// See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_tasks_queue#max_concurrent_dispatches
variable "max_concurrent_dispatches" {
  type    = number
  default = 1
}

// Maps to `max_dispatches_per_second` in the `google_cloud_tasks_queue` resource.
// See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_tasks_queue#max_dispatches_per_second
variable "max_dispatches_per_second" {
  type    = number
  default = 1
}

// Maps to `retry_config.max_attempts` in the `google_cloud_tasks_queue` resource.
// See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_tasks_queue#max_attempts
variable "max_retry_attempts" {
  type    = number
  default = 5
}

// Maps to `stackdriver_logging_config.sampling_ratio` in the `google_cloud_tasks_queue` resource.
// See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_tasks_queue#sampling_ratio
variable "logging_sampling_ratio" {
  type    = number
  default = 1.0
}
