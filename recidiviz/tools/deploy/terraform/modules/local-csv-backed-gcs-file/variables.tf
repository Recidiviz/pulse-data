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

# The project id associated with the resources (ex: "recidiviz-123").
variable "project_id" {
  type = string
}

# The name of the bucket that the file should be loaded into (ex: "recidiviz-123-external-reference-data")
variable "bucket_name" {
  type = string
}

# The name of the file to load (ex: "county_fips.csv")
# This file must be present in the recidiviz/datasets/static_data/ directory.
variable "file_name" {
  type = string
}

# Path to the root of the recidiviz repo
variable "recidiviz_root" {
  type = string
}
