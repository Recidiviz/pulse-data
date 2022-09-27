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

resource "google_artifact_registry_repository" "case_triage_pathways" {
  provider      = google-beta
  location      = "us"
  repository_id = "case-triage-pathways"
  description   = "Repository for docker images for Case Triage / Pathways. Contractors may have access to images in this repository."
  format        = "DOCKER"

  depends_on = [
    google_project_service.artifact_registry_api
  ]
}
