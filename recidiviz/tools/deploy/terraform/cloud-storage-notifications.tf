locals {
}

module "dashboard_data" {
  source = "./modules/cloud-storage-notification"

  bucket_name                = module.dashboard-event-level-data.name
  push_endpoint              = "${local.application_data_import_url}/import/trigger_pathways"
  service_account_email      = google_service_account.application_data_import_cloud_run.email
  filter                     = "NOT hasPrefix(attributes.objectId, \"staging/\") AND NOT hasPrefix(attributes.objectId, \"sandbox/\")"
  minimum_backoff            = "180s"
  maximum_backoff            = "600s"
  message_retention_duration = "86400s"
}
moved {
  from = module.dashboard_data[0]
  to   = module.dashboard_data
}

module "archive_practices_file" {
  source = "./modules/cloud-storage-notification"

  bucket_name           = module.practices-etl-data.name
  push_endpoint         = "${local.application_data_import_url}/practices-etl/archive-file"
  service_account_email = google_service_account.application_data_import_cloud_run.email
}

module "import_ingested_product_users" {
  source = "./modules/cloud-storage-notification"

  bucket_name           = module.product-user-import-bucket.name
  push_endpoint         = "${local.application_data_import_url}/auth/import_ingested_users_async"
  service_account_email = google_service_account.application_data_import_cloud_run.email
  filter                = "NOT hasPrefix(attributes.objectId, \"staging/\") AND NOT hasPrefix(attributes.objectId, \"sandbox/\")"
}

module "handle_workflows_firestore_etl" {
  source = "./modules/cloud-storage-notification"

  bucket_name           = module.practices-etl-data.name
  push_endpoint         = "${local.application_data_import_url}/practices-etl/handle_workflows_firestore_etl"
  service_account_email = google_service_account.application_data_import_cloud_run.email

  suffix = "workflows-firestore-etl"
}

module "handle_insights_etl" {
  source = "./modules/cloud-storage-notification"

  bucket_name                = module.insights-etl-data.name
  push_endpoint              = "${local.application_data_import_url}/import/trigger_insights"
  service_account_email      = google_service_account.application_data_import_cloud_run.email
  filter                     = "NOT hasPrefix(attributes.objectId, \"staging/\") AND NOT hasPrefix(attributes.objectId, \"sandbox/\")"
  minimum_backoff            = "180s"
  maximum_backoff            = "600s"
  message_retention_duration = "86400s"
}

module "handle_insights_etl_demo" {
  count  = var.project_id == "recidiviz-staging" ? 1 : 0
  source = "./modules/cloud-storage-notification"

  bucket_name                = module.insights-etl-data-demo[0].name
  push_endpoint              = "${local.application_data_import_url}/import/trigger_insights"
  service_account_email      = google_service_account.application_data_import_cloud_run.email
  filter                     = "NOT hasPrefix(attributes.objectId, \"staging/\") AND NOT hasPrefix(attributes.objectId, \"sandbox/\")"
  minimum_backoff            = "180s"
  maximum_backoff            = "600s"
  message_retention_duration = "86400s"
}

module "archive_insights_file" {
  source = "./modules/cloud-storage-notification"

  bucket_name           = module.insights-etl-data.name
  push_endpoint         = "${local.application_data_import_url}/outliers-utils/archive-file"
  service_account_email = google_service_account.application_data_import_cloud_run.email

  suffix = "archive-files"
}

module "handle_pulic_pathways_etl" {
  source = "./modules/cloud-storage-notification"

  bucket_name                = module.public-pathways-data.name
  push_endpoint              = "${local.application_data_import_url}/import/trigger_public_pathways"
  service_account_email      = google_service_account.application_data_import_cloud_run.email
  filter                     = "NOT hasPrefix(attributes.objectId, \"staging/\") AND NOT hasPrefix(attributes.objectId, \"sandbox/\")"
  minimum_backoff            = "180s"
  maximum_backoff            = "600s"
  message_retention_duration = "86400s"
}


locals {
  application_data_import_url = google_cloud_run_service.application-data-import.status.0.url
}
