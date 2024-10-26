# Enable Discovery Engine
resource "google_project_service" "discovery_engine_api" {
  project                    = var.project_id
  service                    = "discoveryengine.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = true
}
