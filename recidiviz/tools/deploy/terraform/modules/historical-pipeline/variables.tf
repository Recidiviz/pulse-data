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

variable "project_id" {
  type = string
}

variable "git_hash" {
  type = string
}

variable "pipeline_type" {
  // Either "supervision" or "incarceration"
  type = string
}

variable "state_code" {
  // Of the form "US_XX"
  type = string
}

variable "region" {
  type = string
}

locals {
  lower_state_code        = lower(var.state_code)
  lower_dashed_state_code = replace(local.lower_state_code, "_", "-")

  template_name = "${local.lower_dashed_state_code}-historical-${var.pipeline_type}-calculations-240"
}
