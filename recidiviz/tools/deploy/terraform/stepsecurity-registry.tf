# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
# Shared basic-auth credential for the StepSecurity PyPI registry that uv
# resolves and downloads packages through (see [[tool.uv.index]] in
# pyproject.toml). Engineers read it in initial_setup.sh, and the staging
# release trigger reads it to build images. GitHub Actions reads a hand-set copy
# from the STEPSECURITY_REGISTRY_USERNAME and STEPSECURITY_REGISTRY_PASSWORD org
# secrets, so a rotation must update both places.
#
# Images are only built in recidiviz-staging (production copies them), so the
# secrets exist there only.

locals {
  stepsecurity_registry_enabled = var.project_id == "recidiviz-staging"
  stepsecurity_registry_secret_ids = local.stepsecurity_registry_enabled ? toset([
    "stepsecurity_registry_username",
    "stepsecurity_registry_password",
  ]) : toset([])
  # Engineers need no binding here. Each engineering team's standing baseline
  # role on recidiviz-staging (rzDevBaseline, rzImplementationBaseline,
  # rzDataAnalystBaseline, rzDataScienceBaseline, managed in recidiviz-iam)
  # grants secretmanager.versions.access on the whole project. If a baseline
  # role drops that permission, its team needs a group binding added here.
  stepsecurity_registry_secret_readers = local.stepsecurity_registry_enabled ? toset([
    # The staging release trigger in build-trigger.tf sets no service account,
    # so its builds run as the project's legacy Cloud Build service account.
    "serviceAccount:${data.google_project.current.number}@cloudbuild.gserviceaccount.com",
  ]) : toset([])
}

# Terraform creates the secrets without values; add them as secret versions by
# hand with gcloud secrets versions add.
resource "google_secret_manager_secret" "stepsecurity_registry" {
  for_each  = local.stepsecurity_registry_secret_ids
  secret_id = each.key
  # The gcp.resourceLocations org policy allows US locations only, and it
  # rejects auto replication because auto replication counts as global.
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
      replicas {
        location = "us-east1"
      }
    }
  }
}

resource "google_secret_manager_secret_iam_member" "stepsecurity_registry_access" {
  for_each = {
    for pair in setproduct(local.stepsecurity_registry_secret_ids, local.stepsecurity_registry_secret_readers) :
    "${pair[0]} ${pair[1]}" => { secret_id = pair[0], member = pair[1] }
  }
  secret_id = google_secret_manager_secret.stepsecurity_registry[each.value.secret_id].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}
