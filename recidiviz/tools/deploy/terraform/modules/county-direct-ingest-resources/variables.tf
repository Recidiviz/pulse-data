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

# The county code associated with the buckets and cloud task queues (ex: "US_TX_BRAZOS").
variable "county_code" {
  type = string
}

# the project id associated with the buckets and cloud tasks queues (ex: "recidiviz-123").
variable "project_id" {
  type = string
}

# The preferred region for the buckets and cloud task queues (ex: "us-east4").
variable "region" {
  type = string
}

locals {
  lower_county_code       = replace(lower(var.county_code), "_", "-")
  direct_ingest_county_str = "direct-ingest-county-${local.lower_county_code}"
}
