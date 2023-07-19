# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

resource "google_project_service" "artifact_registry_api" {
  service = "artifactregistry.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

locals {
  config = yamldecode(file("config/artifact_registry_repositories.yaml"))
}

resource "google_artifact_registry_repository" "repositories" {
  provider      = google-beta
  for_each      = tomap(local.config.repositories)
  location      = each.value.location
  repository_id = each.value.repository_id
  description   = each.value.description
  format        = each.value.format

  depends_on = [
    google_project_service.artifact_registry_api
  ]
}


moved {
  from = google_artifact_registry_repository.asset_generation
  to   = google_artifact_registry_repository.repositories["asset_generation"]
}


moved {
  from = google_artifact_registry_repository.case_triage_pathways
  to   = google_artifact_registry_repository.repositories["case_triage_pathways"]
}
