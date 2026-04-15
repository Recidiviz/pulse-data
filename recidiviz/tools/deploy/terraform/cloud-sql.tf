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
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "case_triage"
  instance_name    = var.project_id == "recidiviz-staging" ? "dev-case-triage-data-0af0a" : "prod-case-triage-data"
  database_version = "POSTGRES_13"
  region           = var.us_central_region
  zone             = var.zone
  tier             = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
}

module "justice_counts_database" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "justice_counts"
  database_version = "POSTGRES_13"
  region           = var.us_east_region
  zone             = "us-east1-c"
  tier             = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
}

module "operations_database_v2" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "operations_v2"
  instance_name    = var.project_id == "recidiviz-staging" ? "dev-operations-data-0x17a1b" : "prod-operations-data-0xf04e58"
  database_version = "POSTGRES_13"
  region           = var.us_east_region
  zone             = "us-east1-b"
  tier             = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
  insights_config  = null
}

module "pathways_database" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "pathways"
  database_version = "POSTGRES_13"
  region           = var.us_central_region
  zone             = var.zone
  secondary_zone   = "us-central1-b"
  tier             = coalesce(var.default_sql_tier, "db-custom-4-16384") # 4 vCPUs, 16GB Memory

  additional_databases = [
    for value in local.pathways_enabled_states :
    lower(value)
  ]
}

module "insights_database" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "insights"
  database_version = "POSTGRES_13"
  region           = var.us_central_region
  zone             = var.zone
  secondary_zone   = "us-central1-b"
  tier = coalesce(
    var.default_sql_tier,
    var.project_id == "recidiviz-staging" ? "db-custom-1-3840" : "db-custom-2-8192"
  ) # staging: 1 vCPU, 3.75GB Memory production: 2 vCPUs, 8GB Memory

  additional_databases = [
    for value in local.outliers_enabled_states :
    lower(value)
  ]
}

module "workflows_database" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "workflows"
  database_version = "POSTGRES_13"
  region           = var.us_central_region
  zone             = var.zone
  secondary_zone   = "us-central1-b"
  tier = coalesce(
    var.default_sql_tier,
    var.project_id == "recidiviz-staging" ? "db-custom-1-3840" : "db-custom-2-8192"
  ) # staging: 1 vCPU, 3.75GB Memory production: 2 vCPUs, 8GB Memory

  additional_databases = [
    for value in local.workflows_enabled_states :
    lower(value)
  ]

}

module "persistence_database" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "persistence"
  instance_name  = "persistence-data"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = "us-central1-b"

  tier = coalesce(
    var.default_sql_tier,
    var.project_id == "recidiviz-staging" ? "db-custom-1-3840" : "db-custom-2-8192"
  ) # staging: 1 vCPU, 3.75GB Memory production: 2 vCPUs, 8GB Memory

}

module "public_pathways_database" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "public_pathways"
  database_version = "POSTGRES_13"
  region           = var.us_central_region
  zone             = var.zone
  tier = coalesce(
    var.default_sql_tier,
    var.project_id == "recidiviz-staging" ? "db-custom-1-3840" : "db-custom-2-8192"
  ) # staging: 1 vCPU, 3.75GB Memory production: 2 vCPUs, 8GB Memory
  additional_databases = [
    for value in local.public_pathways_enabled_states :
    lower(value)
  ]
}

locals {
  # Add demo states to staging for demo mode
  pathways_enabled_states        = concat(yamldecode(file("${path.module}/config/pathways_enabled_states.yaml")), var.project_id == "recidiviz-staging" ? ["US_OZ"] : [])
  outliers_enabled_states        = yamldecode(file("${path.module}/config/outliers_enabled_states.yaml"))
  workflows_enabled_states       = yamldecode(file("${path.module}/config/workflows_enabled_states.yaml"))
  public_pathways_enabled_states = concat(yamldecode(file("${path.module}/config/public_pathways_enabled_states.yaml")), var.project_id == "recidiviz-staging" ? ["US_OZ"] : [])

  joined_connection_string = join(
    ",",
    [
      module.case_triage_database.connection_name,
      module.justice_counts_database.connection_name,
      module.pathways_database.connection_name,
      module.workflows_database.connection_name,
      module.insights_database.connection_name,
      module.persistence_database.connection_name,
      # v2 modules
      module.operations_database_v2.connection_name,
      # TODO(Recidiviz/justice-counts#1019): Remove this when the admin panel no longer needs to access the JC database
      var.project_id == "recidiviz-123" ? "justice-counts-production:us-central1:prod-justice-counts-data" : "justice-counts-staging:us-central1:dev-justice-counts-data"
    ]
  )

  # SQL connections to databases that will be imported into via the application-data-import service.
  application_data_connection_string = join(
    ", ",
    [
      module.case_triage_database.connection_name,
      module.pathways_database.connection_name,
      module.insights_database.connection_name,
      module.public_pathways_database.connection_name,
    ]
  )

  public_pathways_connection_string = module.public_pathways_database.connection_name
}
