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

module "archive_insights_file" {
  source = "./modules/cloud-storage-notification"

  bucket_name           = module.insights-etl-data.name
  push_endpoint         = "${local.application_data_import_url}/outliers-utils/archive-file"
  service_account_email = google_service_account.application_data_import_cloud_run.email

  suffix = "archive-files"
}


locals {
  app_engine_url = "https://${var.project_id}.appspot.com"
  # These client IDs come from the app engine service we want to authenticate to, and can be found
  # at https://console.cloud.google.com/apis/credentials (IAP-App-Engine-app)
  app_engine_iap_client = local.is_production ? "688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.googleusercontent.com" : "984160736970-flbivauv2l7sccjsppe34p7436l6890m.apps.googleusercontent.com"
  application_data_import_url = google_cloud_run_service.application-data-import.status.0.url
}
