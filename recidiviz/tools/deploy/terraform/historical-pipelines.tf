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

module "incarceration_historical_pipelines" {
  source = "./modules/historical-pipeline"
  for_each = {
    US_ID = "us-west1"
    US_MO = "us-west3"
    US_ND = "us-east1"
    US_PA = "us-central1"
  }

  project_id    = var.project_id
  git_hash      = var.git_hash
  pipeline_type = "incarceration"
  state_code    = each.key
  region        = each.value
}

module "supervision_historical_pipelines" {
  source = "./modules/historical-pipeline"
  for_each = {
    US_ID = "us-west1"
    US_MO = "us-west3"
    US_ND = "us-east1"
    US_PA = "us-central1"
  }

  project_id    = var.project_id
  git_hash      = var.git_hash
  pipeline_type = "supervision"
  state_code    = each.key
  region        = each.value
}
