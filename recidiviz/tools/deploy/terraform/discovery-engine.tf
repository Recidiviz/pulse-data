# Enable Discovery Engine
resource "google_project_service" "discovery_engine_api" {
  project                    = var.project_id
  service                    = "discoveryengine.googleapis.com"
  disable_dependent_services = false
  disable_on_destroy         = false
}
