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
  description = "Builds a remote Docker image for staging on every push to master."
  count       = var.project_id == "recidiviz-staging" ? 1 : 0

  github {
    owner = "Recidiviz"
    name  = "pulse-data"
    push {
      branch = "^master$|^releases/v[0-9]+.[0-9]+-rc$"
    }
  }

  build {
    step {
      name = "gcr.io/kaniko-project/executor:v1.6.0"
      args = ["--destination=us.gcr.io/$PROJECT_ID/appengine/build:$COMMIT_SHA", "--cache=true"]
    }
    options {
      machine_type = "N1_HIGHCPU_32"
    }
    timeout = "3600s"
  }
}
