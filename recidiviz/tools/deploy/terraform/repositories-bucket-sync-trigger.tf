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

# Mirrors the pulse-data working tree to the analyst Colab support bucket,
# gs://recidiviz-staging-repositories/pulse-data, on every push to main.
# Analyst notebooks gcsfuse-mount that bucket and `%run` files out of the
# checkout, so it must track main. This replaces a manual `gsutil rsync` that
# was last run on 2026-04-16 and then silently went stale, leaving the mirror
# months behind main.
#
# Staging-only (`count` guard): the bucket lives in recidiviz-staging and has no
# prod equivalent. The default Cloud Build service account already has GCS write
# access to the project's buckets (roles/cloudbuild.builds.builder), so no
# dedicated service account is required.
resource "google_cloudbuild_trigger" "repositories_bucket_sync" {
  provider    = google-beta
  count       = var.project_id == "recidiviz-staging" ? 1 : 0
  name        = "sync-pulse-data-to-repositories-bucket"
  description = "Mirrors the pulse-data working tree to gs://recidiviz-staging-repositories/pulse-data on every push to main."

  github {
    owner = "Recidiviz"
    name  = "pulse-data"
    push {
      branch = "^main$"
    }
  }

  build {
    # rsync the checked-out working tree into the bucket's pulse-data/ prefix.
    #   --delete-unmatched-destination-objects  keeps the mirror true (files
    #       removed from the repo are removed from the mirror). It is scoped to
    #       the pulse-data/ prefix, so sibling objects in the bucket (fonts,
    #       geocoded CSVs, colab_directory/, recidiviz-research/) are untouched.
    #   --exclude=(^|/)\.git/  keeps the ~500MB of git history out of the mirror;
    #       notebooks only need the working tree. (.gitignore/.github are kept.)
    step {
      name       = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"
      entrypoint = "gcloud"
      args = [
        "storage",
        "rsync",
        ".",
        "gs://recidiviz-staging-repositories/pulse-data",
        "--recursive",
        "--delete-unmatched-destination-objects",
        "--exclude=(^|/)\\.git/",
      ]
      id = "rsync-working-tree-to-repositories-bucket"
    }

    timeout = "1800s"
  }
}
