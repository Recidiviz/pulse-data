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

module "case_triage_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "case_triage"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = var.project_id == "recidiviz-staging" ? "us-central1-f" : "us-central1-b"
  tier           = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory

}

module "justice_counts_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "justice_counts"
  region         = var.us_east_region
  zone           = "us-east1-c"
  secondary_zone = var.project_id == "recidiviz-staging" ? "us-east1-b" : "us-east1-d"
  tier           = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory

}

module "operations_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id       = var.project_id
  instance_key     = "operations"
  base_secret_name = "operations_v2"
  region           = var.us_east_region
  zone             = "us-east1-b"
  secondary_zone   = "us-east1-c"
  tier             = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
  insights_config  = null

}

module "pathways_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "pathways"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = "us-central1-b"
  tier           = coalesce(var.default_sql_tier, "db-custom-4-16384") # 4 vCPUs, 16GB Memory


  additional_databases = [
    for value in local.pathways_enabled_states :
    lower(value)
  ]
}

module "insights_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "insights"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = "us-central1-b"
  tier           = coalesce(var.default_sql_tier, "db-custom-2-8192") # 2 vCPUs, 8GB Memory


  additional_databases = [
    for value in local.outliers_enabled_states :
    lower(value)
  ]
}

module "workflows_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "workflows"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = "us-central1-b"
  tier           = coalesce(var.default_sql_tier, "db-custom-2-8192") # 2 vCPUs, 8GB Memory


  additional_databases = [
    for value in local.workflows_enabled_states :
    lower(value)
  ]
}

module "persistence_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "persistence"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = "us-central1-b"


  tier = coalesce(
    var.default_sql_tier,
    var.project_id == "recidiviz-staging" ? "db-custom-1-3840" : "db-custom-2-8192"
  ) # staging: 1 vCPU, 3.75GB Memory production: 2 vCPUs, 8GB Memory
}

module "public_pathways_database_cmek" {
  source = "./modules/cloud-sql-instance"

  project_id     = var.project_id
  instance_key   = "public_pathways"
  region         = var.us_central_region
  zone           = var.zone
  secondary_zone = "us-central1-c"


  tier = coalesce(
    var.default_sql_tier,
    var.project_id == "recidiviz-staging" ? "db-custom-1-3840" : "db-custom-2-8192"
  ) # staging: 1 vCPU, 3.75GB Memory production: 2 vCPUs, 8GB Memory

  additional_databases = [
    for value in local.public_pathways_enabled_states :
    lower(value)
  ]
}


module "identity_service_database" {
  source = "./modules/cloud-sql-instance"

  project_id   = var.project_id
  instance_key = "identity_service"
  region       = var.us_central_region
  zone         = var.zone
  tier         = coalesce(var.default_sql_tier, "db-custom-1-3840") # 1 vCPU, 3.75GB Memory
}

# Grant Cloud SQL per-instance service accounts cryptoKeyEncrypterDecrypter on
# the cloud-sql-exports bucket's CMEK key. Required because SQL export operations
# write to this bucket using the per-instance SA, and CMEK-encrypted buckets
# require the writer to have key access.
#
# When adding a new Cloud SQL instance above, add it here too — the missing grant
# won't surface until that instance tries to export to GCS.
locals {
  sql_instances_needing_gcs_cmek_access = {
    case_triage_cmek     = module.case_triage_database_cmek
    justice_counts_cmek  = module.justice_counts_database_cmek
    operations_cmek      = module.operations_database_cmek
    pathways_cmek        = module.pathways_database_cmek
    insights_cmek        = module.insights_database_cmek
    workflows_cmek       = module.workflows_database_cmek
    persistence_cmek     = module.persistence_database_cmek
    public_pathways_cmek = module.public_pathways_database_cmek
    identity_service     = module.identity_service_database
  }
}

resource "google_kms_crypto_key_iam_member" "sql_sa_gcs_cmek_access" {
  for_each      = local.sql_instances_needing_gcs_cmek_access
  crypto_key_id = module.direct-ingest-cloud-sql-exports.cmek_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${each.value.instance_service_account_email}"
}

locals {
  # Add demo states to staging for demo mode
  pathways_enabled_states        = concat(yamldecode(file("${path.module}/config/pathways_enabled_states.yaml")), var.project_id == "recidiviz-staging" ? ["US_OZ"] : [])
  outliers_enabled_states        = yamldecode(file("${path.module}/config/outliers_enabled_states.yaml"))
  workflows_enabled_states       = yamldecode(file("${path.module}/config/workflows_enabled_states.yaml"))
  public_pathways_enabled_states = concat(yamldecode(file("${path.module}/config/public_pathways_enabled_states.yaml")), var.project_id == "recidiviz-staging" ? ["US_OZ"] : [])

  env_prefix = var.project_id == "recidiviz-staging" ? "dev" : "prod"

  joined_connection_string = join(
    ",",
    [
      module.case_triage_database_cmek.connection_name,
      # TODO(Recidiviz/justice-counts#1019): Remove this when the admin panel no longer needs to access the JC database
      module.justice_counts_database_cmek.connection_name,
      module.pathways_database_cmek.connection_name,
      module.workflows_database_cmek.connection_name,
      module.insights_database_cmek.connection_name,
      module.persistence_database_cmek.connection_name,
      module.operations_database_cmek.connection_name,
    ]
  )

  # SQL connections to databases that will be imported into via the application-data-import service.
  application_data_connection_string = join(
    ", ",
    [
      module.case_triage_database_cmek.connection_name,
      module.pathways_database_cmek.connection_name,
      module.insights_database_cmek.connection_name,
      module.public_pathways_database_cmek.connection_name,
    ]
  )

  public_pathways_connection_string = join(
    ", ",
    [
      module.public_pathways_database_cmek.connection_name,
    ]
  )
}
