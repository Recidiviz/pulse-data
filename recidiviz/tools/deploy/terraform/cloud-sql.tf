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

  project_id        = var.project_id
  instance_key      = "case_triage"
  base_secret_name  = "case_triage"
  region            = var.region
  zone              = var.zone
  tier              = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
  has_readonly_user = true
  insights_config = {
    query_insights_enabled  = true
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}

module "justice_counts_database" {
  source = "./modules/cloud-sql-instance"

  project_id        = var.project_id
  instance_key      = "justice_counts"
  base_secret_name  = "justice_counts"
  region            = "us-east1"
  zone              = "us-east1-c"
  tier              = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
  has_readonly_user = local.is_production
  insights_config = {
    query_insights_enabled  = true
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}


module "operations_database_v2" {
  source = "./modules/cloud-sql-instance"

  project_id        = var.project_id
  instance_key      = "operations_v2"
  base_secret_name  = "operations_v2"
  region            = "us-east1"
  zone              = "us-east1-b"
  tier              = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
  has_readonly_user = true
}

module "pathways_database" {
  source = "./modules/cloud-sql-instance"

  project_id        = var.project_id
  instance_key      = "pathways"
  base_secret_name  = "pathways"
  region            = var.region
  zone              = var.zone
  secondary_zone    = "us-central1-b"
  tier              = coalesce(var.default_sql_tier, "db-custom-4-16384") # 4 vCPUs, 16GB Memory
  has_readonly_user = true

  additional_databases = [
    for value in local.pathways_enabled_states :
    lower(value)
  ]
  insights_config = {
    query_insights_enabled  = true
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}

module "outliers_database" {
  source = "./modules/cloud-sql-instance"

  project_id        = var.project_id
  instance_key      = "outliers"
  base_secret_name  = "outliers"
  region            = var.region
  zone              = var.zone
  secondary_zone    = "us-central1-b"
  tier              = coalesce(var.default_sql_tier, "db-custom-4-16384") # 4 vCPUs, 16GB Memory
  has_readonly_user = true

  additional_databases = [
    for value in local.outliers_enabled_states :
    lower(value)
  ]
  insights_config = {
    query_insights_enabled  = true
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}

module "workflows_database" {
  source = "./modules/cloud-sql-instance"

  project_id        = var.project_id
  instance_key      = "workflows"
  base_secret_name  = "workflows"
  region            = var.region
  zone              = var.zone
  secondary_zone    = "us-central1-b"
  tier              = coalesce(var.default_sql_tier, "db-custom-4-16384") # 4 vCPUs, 16GB Memory
  has_readonly_user = true

  additional_databases = [
    for value in local.workflows_enabled_states :
    lower(value)
  ]

  insights_config = {
    query_insights_enabled  = true
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}

locals {
  # Add demo states to staging for demo mode
  pathways_enabled_states  = concat(yamldecode(file("${path.module}/config/pathways_enabled_states.yaml")), var.project_id == "recidiviz-staging" ? ["US_OZ"] : [])
  outliers_enabled_states  = yamldecode(file("${path.module}/config/outliers_enabled_states.yaml"))
  workflows_enabled_states = yamldecode(file("${path.module}/config/workflows_enabled_states.yaml"))

  joined_connection_string = join(
    ",",
    [
      module.case_triage_database.connection_name,
      module.justice_counts_database.connection_name,
      module.pathways_database.connection_name,
      module.outliers_database.connection_name,
      module.workflows_database.connection_name,
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
      module.outliers_database.connection_name,
    ]
  )
}
