# Identity-Aware Proxy (IAP) Configuration
# OAuth credentials must be created manually in GCP Console and stored in SOPS config
# See example config file for setup instructions

# Enable IAP API
resource "google_project_service" "iap" {
  count = local.iap_enabled ? 1 : 0

  project = var.project_id
  service = "iap.googleapis.com"

  disable_on_destroy = false
}

# IAM binding to grant IAP access to specified members
# This controls who can authenticate through IAP after OAuth login
# Note: Using regional IAM binding since we have a regional backend service
resource "google_iap_web_region_backend_service_iam_binding" "sftpgo_admin_access" {
  count = local.iap_enabled && length(local.iap_access_members) > 0 ? 1 : 0

  project                    = var.project_id
  region                     = var.region
  web_region_backend_service = google_compute_region_backend_service.sftpgo.name
  role                       = "roles/iap.httpsResourceAccessor"
  members                    = local.iap_access_members

  depends_on = [
    google_project_service.iap,
    google_compute_region_backend_service.sftpgo
  ]
}
