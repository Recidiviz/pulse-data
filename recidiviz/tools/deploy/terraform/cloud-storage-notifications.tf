module "dashboard_data" {
  # TODO(#12449): Remove staging-only check when we create production environment
  count  = var.project_id == "recidiviz-123" ? 0 : 1
  source = "./modules/cloud-storage-notification"

  bucket_name   = module.dashboard-event-level-data.name
  push_endpoint = "${google_cloud_run_service.application-data-import[count.index].status.0.url}/import/trigger_pathways"
}
