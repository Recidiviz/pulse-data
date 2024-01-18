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

module "state-specific-scratch-dataset" {
  count       = var.is_production ? 0 : 1
  source      = "../big_query_dataset"
  dataset_id  = "${lower(var.state_code)}_scratch"
  description = "State-specific scratch space dataset that can be used to save one-off queries related to ${upper(var.state_code)} data. May provide a temporary staging ground for some ingest external validation data."
}

# TODO(#13312): Move all one off validation data for these states into `us_xx_validation_oneoffs`
# and have `us_xx_validation` only contain version controlled views pulling from oneoffs and raw data.
module "state-specific-validation-dataset" {
  count       = contains(["US_ID", "US_ND", "US_PA", "US_TN"], var.state_code) ? 1 : 0 
  source      = "../big_query_dataset"
  dataset_id  = "${lower(var.state_code)}_validation"
  description = "State-specific validation dataset for state-specific validation views."
}

moved {
  from = module.state-specific-validation-dataset
  to   = module.state-specific-validation-dataset[0]
}

module "state-specific-validation-oneoff-dataset" {
  source      = "../big_query_dataset"
  dataset_id  = "${lower(var.state_code)}_validation_oneoffs"
  description = "State-specific validation oneoffs dataset for external data that is sent directly from the state."
}
