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

#TODO(#8450): Decode metadata by pipeline & state directly from `production_calculation_pipeline_templates.yaml`
module "incarceration_historical_pipelines" {
  source = "./modules/historical-pipeline"
  for_each = {
    US_ID = {
        region = "us-west1"
        calculation_month_count = 240
    }
    US_MO = {
        region = "us-west3"
        calculation_month_count = 240
    }
    US_ND = {
        region = "us-east1"
        calculation_month_count = 240
    }
    US_PA = {
        region = "us-central1"
        calculation_month_count = 240
    }
  }

  project_id     = var.project_id
  repo_url       = local.repo_url
  pipeline_type  = "incarceration"
  state_code     = each.key
  region         = each.value.region
  calculation_month_count = each.value.calculation_month_count
}

module "supervision_historical_pipelines" {
  source = "./modules/historical-pipeline"
  for_each = {
    US_ID = {
        region = "us-west1"
        calculation_month_count = 240
    }
    US_MO = {
        region = "us-west3"
        calculation_month_count = 240
    }
    US_ND = {
        region = "us-east1"
        calculation_month_count = 240
    }
    US_PA = {
        region = "us-central1"
        calculation_month_count = 240
    }
  }

  project_id     = var.project_id
  repo_url       = local.repo_url
  pipeline_type  = "supervision"
  state_code     = each.key
  region         = each.value.region
  calculation_month_count = each.value.calculation_month_count
}

module "violation_historical_pipelines" {
  source = "./modules/historical-pipeline"
  for_each = {
    US_ID = {
        region = "us-west1"
        calculation_month_count = 240
    }
    US_MO = {
        region = "us-west3"
        calculation_month_count = 240
    }
    US_PA = {
        region = "us-central1"
        calculation_month_count = 240
    }
  }

  project_id     = var.project_id
  repo_url       = local.repo_url
  pipeline_type  = "violation"
  state_code     = each.key
  region         = each.value.region
  calculation_month_count = each.value.calculation_month_count
}

module "program_historical_pipelines" {
  source = "./modules/historical-pipeline"
  for_each = {
    US_ND = {
        region = "us-east1"
        calculation_month_count = 60
    }
  }

  project_id     = var.project_id
  repo_url       = local.repo_url
  pipeline_type  = "program"
  state_code     = each.key
  region         = each.value.region
  calculation_month_count = each.value.calculation_month_count
}
