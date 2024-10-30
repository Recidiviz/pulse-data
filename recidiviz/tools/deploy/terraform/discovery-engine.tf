# Enable Discovery Engine
resource "google_project_service" "discovery_engine_api" {
  project                    = var.project_id
  service                    = "discoveryengine.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = true
}

resource "google_discovery_engine_data_store" "case_notes" {
  project           = var.project_id
  data_store_id     = "case-notes"
  location          = "us"
  industry_vertical = "GENERIC"
  display_name      = "Case Notes Data Store"
  content_config    = "CONTENT_REQUIRED"
  solution_types    = ["SOLUTION_TYPE_SEARCH"]

  depends_on = [
    google_project_service.discovery_engine_api
  ]
}

resource "google_discovery_engine_search_engine" "case_notes_search" {
  project        = var.project_id
  engine_id      = "case-notes-search"
  location       = google_discovery_engine_data_store.case_notes.location
  display_name   = "Case Notes Search"
  collection_id  = "default_collection"
  data_store_ids = [google_discovery_engine_data_store.case_notes.data_store_id]
  search_engine_config {
    search_tier = "SEARCH_TIER_ENTERPRISE"
  }
  common_config {
    company_name = "Recidiviz"
  }

  depends_on = [
    google_project_service.discovery_engine_api
  ]
}
