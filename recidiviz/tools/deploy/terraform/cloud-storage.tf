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
  po_report_static_path  = format("%s/reporting/context/static", local.recidiviz_root)
  po_report_static_files = fileset(local.po_report_static_path, "./**/*")
}

module "justice-counts-data-bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "justice-counts-data"
  use_cmek    = true
}

module "justice-counts-ingest" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "justice-counts-ingest"
  use_cmek    = true
}

# This "storage bucket" for raw data holds processed and deprecated
# data for each state in the "PRIMARY" instance.
# Note that var.direct_ingest_region is a GCP region not to be confused with 
# a "direct ingest region" comprised of a state code and instance elsewhere in the codebase.
module "direct-ingest-state-storage" {
  source = "./modules/cloud-storage-bucket"

  project_id    = var.project_id
  location      = var.direct_ingest_region
  storage_class = "NEARLINE"
  name_suffix   = "direct-ingest-state-storage"
  use_cmek      = true
}

# This "storage bucket" for raw data holds processed and deprecated
# data for each state in the "SECONDARY" instance.
# Note that var.direct_ingest_region is a GCP region not to be confused with 
# a "direct ingest region" comprised of a state code and instance elsewhere in the codebase.
module "direct-ingest-state-storage-secondary" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  location    = var.direct_ingest_region
  name_suffix = var.direct_ingest_state_storage_secondary_bucket_name_suffix
  use_cmek    = true
}

module "direct-ingest-cloud-sql-exports" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "cloud-sql-exports"
  use_cmek    = true
}

module "direct-ingest-temporary-files" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "direct-ingest-temporary-files"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    },
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 7
      }
    },
  ]
}

module "dashboard-data" {
  source = "./modules/cloud-storage-bucket"

  project_id    = var.project_id
  name_suffix   = "dashboard-data"
  storage_class = "MULTI_REGIONAL"
  use_cmek      = true
}

module "public-pathways-data" {
  source = "./modules/cloud-storage-bucket"

  project_id    = var.project_id
  name_suffix   = "public-pathways-data"
  storage_class = "MULTI_REGIONAL"
  use_cmek      = true
}

module "dashboard-event-level-data" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "dashboard-event-level-data"
  use_cmek    = true
}

module "dashboard-user-restrictions-bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "dashboard-user-restrictions"
  use_cmek    = true
}

module "product-user-import-bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "product-user-import"
  use_cmek    = true
}

# TODO(#13703): Figure out what we want to do with this data and whether we still want
#  to manage this bucket, which contains data from deprecated state aggregate scrapers,
#  via TF.
module "state-aggregate-reports" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "state-aggregate-reports"
  use_cmek    = true
}

module "case-triage-data" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "case-triage-data"
  uniform_bucket_level_access = false
  use_cmek                    = true
}

module "covid-dashboard-data" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "covid-dashboard-data"
  uniform_bucket_level_access = false
  use_cmek                    = true
}

module "configs" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "configs"
  uniform_bucket_level_access = false
  use_cmek                    = true
}

# Ensure we have an empty JSON file in storage somewhere so we can use it as the data source for
# schema-only BigQuery copies of tables with external data configurations.
resource "google_storage_bucket_object" "empty_json" {
  name   = "empty.json"
  bucket = module.configs.name
  source = "${local.recidiviz_root}/datasets/static_data/empty.json"
}

module "dbexport" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "dbexport"
  location                    = "us-east4"
  storage_class               = "REGIONAL"
  uniform_bucket_level_access = false
  use_cmek                    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "dataflow-templates" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "dataflow-templates"
  storage_class               = "MULTI_REGIONAL"
  uniform_bucket_level_access = false
  use_cmek                    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 5
      }
    },
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 21
      }
    },
  ]
}

module "gcslock" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "gcslock"
  uniform_bucket_level_access = false
  use_cmek                    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 3
      }
    },
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 1
      }
    },
  ]
}

module "ingest-metadata" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "ingest-metadata"
  uniform_bucket_level_access = false
  use_cmek                    = true
}

module "practices-etl-data" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "practices-etl-data"
  uniform_bucket_level_access = false
  use_cmek                    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 7
      }
    }
  ]
}

module "practices-etl-data-demo" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "practices-etl-data-demo"
  uniform_bucket_level_access = false
  count                       = var.project_id == "recidiviz-staging" ? 1 : 0
  use_cmek                    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 7
      }
    }
  ]
}

module "practices-etl-data-archive" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "practices-etl-data-archive"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "dashboard-firestore-backups" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "dashboard-firestore-backups"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 7
      }
    }
  ]
}

# TODO(#13703): Figure out what we want to do with this data and whether we still want
#  to manage this bucket, which contains data from deprecated state aggregate scrapers,
#  via TF.
module "processed-state-aggregates" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "processed-state-aggregates"
  storage_class               = "MULTI_REGIONAL"
  uniform_bucket_level_access = false
  use_cmek                    = true

  labels = {
    "recidiviz_service" = "scrapers",
    "vanta-owner"       = "joshua",
    "vanta-description" = "terraform-managed-gcs-bucket"
  }

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "public-dashboard-data" {
  source = "./modules/cloud-storage-bucket"

  project_id                  = var.project_id
  name_suffix                 = "public-dashboard-data"
  uniform_bucket_level_access = false
  use_cmek                    = true
}

module "report-data" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "report-data"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 5
      }
    }
  ]
}

module "report-data-archive" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "report-data-archive"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "report-html" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "report-html"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "report-images" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "report-images"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "sendgrid-data" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "sendgrid-data"
  use_cmek    = true
}

module "validation-metadata" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "validation-metadata"
  use_cmek    = true
}

# A temporary staging area where templates are copied to as part of running templates
module "dataflow_templates_temporary" {
  source      = "./modules/cloud-storage-bucket"
  project_id  = var.project_id
  name_suffix = "dataflow-templates-temporary"

  location                    = "us-central1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  use_cmek                    = true

  soft_delete_policy = {
    retention_duration_seconds = 0
  }

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 3
      }
    }
  ]
}

# This bucket contains legacy county direct ingest data from Vera.
module "direct-ingest-county-storage" {
  source = "./modules/cloud-storage-bucket"

  project_id    = var.project_id
  location      = var.direct_ingest_region
  storage_class = "REGIONAL"
  name_suffix   = "direct-ingest-county-storage"
  use_cmek      = true
}

module "outliers-etl-data" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "outliers-etl-data"
  use_cmek    = true
}

module "outliers-etl-data-archive" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "outliers-etl-data-archive"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "insights-etl-data" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "insights-etl-data"
  use_cmek    = true
}

module "insights-etl-data-demo" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "insights-etl-data-demo"
  count       = var.project_id == "recidiviz-staging" ? 1 : 0
  use_cmek    = true
}

module "insights-etl-data-archive" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "insights-etl-data-archive"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 2
      }
    }
  ]
}

module "case-notes-vertex-search-data" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "case-notes-vertex-search-data"
  use_cmek    = true
}

module "generated-assets" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "generated-assets"
  use_cmek    = true
}

resource "google_storage_bucket_iam_member" "asset-generation-generated-assets-bucket-member" {
  for_each = toset(["roles/storage.objectCreator", "roles/storage.objectViewer"])
  bucket   = module.generated-assets.name
  role     = each.key
  member   = "serviceAccount:${google_service_account.asset_generation_cloud_run.email}"
}

resource "google_storage_bucket_object" "po_report_static_files" {
  for_each = local.po_report_static_files
  bucket   = module.report-images.name
  name     = format("po_monthly_report/static/%s", replace(each.key, local.po_report_static_path, ""))
  source   = format("%s/%s", local.po_report_static_path, each.key)
}

module "override-districts-archive" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "override-districts-archive"
  use_cmek    = true
}

module "airflow-kubernetes-pod-operator-outputs" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "airflow-kubernetes-pod-operator-outputs"
  use_cmek    = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 7
      }
    },
  ]
}

module "user-data-downloads" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "user-data-downloads"
  use_cmek    = true
}

module "us-tn-persistence-redirect" {
  source = "./modules/cloud-storage-bucket"

  count = var.project_id == "recidiviz-staging" ? 1 : 0

  project_id  = var.project_id
  name_suffix = "us-tn-persistence-archive"
  use_cmek    = true
}
