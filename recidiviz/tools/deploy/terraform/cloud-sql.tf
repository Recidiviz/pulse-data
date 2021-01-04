
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

module "case_triage_database" {
  source            = "./modules/cloud-sql"
  base_secret_name  = "case_triage"
  region            = var.region
  zone              = var.zone
  tier              = "db-custom-1-3840" # 1 vCPU, 3.75GB Memory
  has_readonly_user = true
  require_ssl_connection = true
}

