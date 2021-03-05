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

locals {
  is_production = var.project_id == "recidiviz-123"
}

module "case_triage_database" {
  source = "./modules/cloud-sql-instance"

  base_secret_name       = "case_triage"
  region                 = var.region
  zone                   = var.zone
  tier                   = "db-custom-1-3840" # 1 vCPU, 3.75GB Memory
  has_readonly_user      = true
  require_ssl_connection = true
}


module "jails_database" {
  source = "./modules/cloud-sql-instance"

  base_secret_name = "sqlalchemy"
  database_version = "POSTGRES_9_6"
  region           = "us-east4"
  zone             = local.is_production ? "us-east4-b" : "us-east4-c"
  # 4 vCPU, 15GB Memory | 1 vCPU, 3.75GB Memory
  tier              = local.is_production ? "db-custom-4-15360" : "db-custom-1-3840"
  has_readonly_user = local.is_production
}


module "justice_counts_database" {
  source = "./modules/cloud-sql-instance"

  base_secret_name               = "justice_counts"
  region                         = "us-east1"
  zone                           = "us-east1-c"
  tier                           = "db-custom-1-3840" # 1 vCPU, 3.75GB Memory
  has_readonly_user              = local.is_production
  point_in_time_recovery_enabled = local.is_production
}


module "operations_database" {
  source = "./modules/cloud-sql-instance"

  base_secret_name  = "operations"
  database_version  = "POSTGRES_11"
  region            = "us-east1"
  zone              = "us-east1-b"
  tier              = "db-custom-1-3840" # 1 vCPU, 3.75GB Memory
  has_readonly_user = local.is_production
}


module "state_database" {
  source = "./modules/cloud-sql-instance"

  base_secret_name  = "state"
  database_version  = "POSTGRES_9_6"
  region            = "us-east1"
  zone              = "us-east1-c"
  tier              = "db-custom-4-16384" # 4 vCPUs, 16GB Memory
  has_readonly_user = local.is_production
}

locals {
  joined_connection_string = join(
    ", ",
    [
      module.case_triage_database.connection_name,
      module.jails_database.connection_name,
      module.justice_counts_database.connection_name,
      module.operations_database.connection_name,
      module.state_database.connection_name,
    ]
  )
}
