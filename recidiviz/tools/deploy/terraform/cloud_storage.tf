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


locals {
  state_alpha_codes = ["TN"]
}
module "state_direct_ingest_buckets_and_accounts" {
  for_each = toset(local.state_alpha_codes)
  source        = "./modules/state-direct-ingest-bucket"
  state_code    = each.key
  region        = var.region
  is_production = local.is_production
  project_id    = var.project_id
}
