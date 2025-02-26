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
resource "google_cloudbuild_trigger" "staging_release_build_trigger" {
  provider    = google-beta
  description = "Builds a remote Docker image for staging on every push to main."
  count       = var.project_id == "recidiviz-staging" ? 1 : 0

  github {
    owner = "Recidiviz"
    name  = "pulse-data"
    push {
      branch = "^main$|^releases/v[0-9]+.[0-9]+-rc$"
    }
  }

  # TODO(#18537): move images from container registry (us.gcr.io) to artifact registry (us-docker.pkg.dev)
  build {
    step {
      name = "gcr.io/kaniko-project/executor:v1.8.1"
      args = ["--destination=us.gcr.io/$PROJECT_ID/appengine/build:$COMMIT_SHA", "--cache=true", "--target=recidiviz-app", "--skip-unused-stages=true"]
      id   = "recidiviz"
    }
    step {
      name     = "gcr.io/kaniko-project/executor:v1.8.1"
      args     = ["--destination=us.gcr.io/$PROJECT_ID/recidiviz-base:latest", "--cache=true", "--dockerfile=Dockerfile.recidiviz-base"]
      id       = "recidiviz-base"
      wait_for = ["-"] # Run this step in parallel with the previous one
    }
    step {
      name     = "gcr.io/kaniko-project/executor:v1.8.1"
      args     = ["--destination=us-docker.pkg.dev/$PROJECT_ID/asset-generation/asset-generation:latest", "--cache=true", "--dockerfile=Dockerfile.asset-generation"]
      id       = "asset-generation"
      wait_for = ["-"] # Run this step in parallel with the previous one
    }
    step {
      name     = "gcr.io/kaniko-project/executor:v1.8.1"
      args     = ["--destination=us-docker.pkg.dev/$PROJECT_ID/case-triage-pathways/case-triage-pathways:latest", "--cache=true", "--dockerfile=Dockerfile.case-triage-pathways"]
      id       = "case-triage-pathways"
      wait_for = ["recidiviz-base"]
    }
    options {
      machine_type = "N1_HIGHCPU_32"
    }
    timeout = "3600s"
  }

  depends_on = [google_artifact_registry_repository.asset_generation]
}
