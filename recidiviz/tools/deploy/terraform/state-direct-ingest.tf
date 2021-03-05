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

# TODO(#6160): Rename this module to better reflect what it actually is doing for us.
module "state_direct_ingest_buckets_and_accounts" {
  for_each = toset(local.direct_ingest_state_codes)
  source   = "./modules/state-direct-ingest-resources"

  state_code       = each.key
  region           = var.direct_ingest_region
  is_production    = local.is_production
  project_id       = var.project_id
  state_admin_role = google_project_iam_custom_role.state-admin-role.name
  git_hash         = var.git_hash
  db_instance_name = module.state_database.instance_name
}
