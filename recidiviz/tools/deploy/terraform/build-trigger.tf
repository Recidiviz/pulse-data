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

# Build steps are generated from Python code. To update, run:
#   python -m recidiviz.tools.deploy.cloud_build.generate_recidiviz_data_post_commit_docker_build_steps
locals {
  image_build_config = yamldecode(file("config/recidiviz_data_post_commit_docker_build_steps.yaml"))
}

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

  build {
    dynamic "step" {
      for_each = local.image_build_config.steps
      content {
        name       = step.value.name
        id         = lookup(step.value, "id", null)
        args       = lookup(step.value, "args", null)
        entrypoint = lookup(step.value, "entrypoint", null)
        env        = lookup(step.value, "env", null)
        wait_for   = lookup(step.value, "wait_for", null)

        dynamic "volumes" {
          for_each = lookup(step.value, "volumes", [])
          content {
            name = volumes.value.name
            path = volumes.value.path
          }
        }
      }
    }
    options {
      machine_type = local.image_build_config.options.machine_type
    }
    timeout = local.image_build_config.timeout
  }

  depends_on = [
    google_artifact_registry_repository.repositories
  ]
}
