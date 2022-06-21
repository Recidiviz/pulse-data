module "dashboard_data" {
  source = "./modules/cloud-storage-notification"

  bucket_name           = module.dashboard-event-level-data.name
  push_endpoint         = "${google_cloud_run_service.application-data-import.status.0.url}/import/trigger_pathways"
  service_account_email = google_service_account.application_data_import_cloud_run.email
  filter                = "NOT hasPrefix(attributes.objectId, \"staging/\")"
}
moved {
  from = module.dashboard_data[0]
  to   = module.dashboard_data
}
