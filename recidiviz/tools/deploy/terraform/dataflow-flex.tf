# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

resource "google_cloudbuild_trigger" "flex_pipelines_docker_image_build_trigger" {
  provider    = google-beta
  description = "Builds a remote Docker image for flex pipelines on every push to main or a release branch."
  # A build trigger is only needed in staging, when we push to prod the existing staging image will just be retagged,
  # instead of pushing the same image to both prod and staging upon every commit.
  count = var.project_id == "recidiviz-staging" ? 1 : 0

  github {
    owner = "Recidiviz"
    name  = "pulse-data"
    push {
      branch = "^main$|^releases/v[0-9]+.[0-9]+-rc$"
    }
  }

  filename = "recidiviz/pipelines/cloudbuild.pipelines.yaml"

}

resource "google_storage_bucket_object" "flex_template_metadata" {
  bucket       = "${var.project_id}-dataflow-flex-templates"
  content_type = "application/json"

  # This line means we will make a new google_storage_bucket_object for each file we find at the given wildcard path
  for_each = fileset("${local.recidiviz_root}/calculator/pipeline/", "*/template_metadata.json")

  # Here we extract the last directory before the filename (e.g. metrics) and append to the filename to make the full file name
  name = "template_metadata/${basename(dirname(each.value))}.json"

  content = jsonencode({
    image = "us-docker.pkg.dev/${var.project_id}/dataflow/default:${var.docker_image_tag}"
    sdkInfo = {
      language = "PYTHON"
    }
    # Here we read the file contents using the full file path
    metadata = jsondecode(file("${local.recidiviz_root}/calculator/pipeline/${each.value}")),
  })
}
