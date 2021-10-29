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

resource "google_bigquery_dataset" "state_specific_scratch_dataset" {
  count       = var.is_production ? 0 : 1
  dataset_id  =  "${lower(var.state_code)}_scratch"
  description = "State-specific scratch space dataset that can be used to save one-off queries related to ${upper(var.state_code)} data. May provide a temporary staging ground for some ingest external validation data."
  location    = "US"
}
