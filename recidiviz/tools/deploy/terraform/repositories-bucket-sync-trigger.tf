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
# The sync itself never enumerates the destination, because doing so against this
# CMEK-encrypted bucket costs one metadata GET per object (see #93765).
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
    # The sync logic lives in a checked-in script rather than inline args: inline
    # bash here would need every `$` doubled to survive Cloud Build substitution —
    # which this trigger runs with ALLOW_LOOSE, so an unescaped variable would
    # silently expand to the empty string rather than fail.
    #
    # The script uploads the tracked tree and then deletes only mirror objects
    # that are no longer tracked in the repo, scoped to the pulse-data/
    # prefix, so sibling objects in the bucket (fonts, geocoded CSVs,
    # colab_directory/, recidiviz-research/) are untouched. It deliberately does
    # not use `gcloud storage rsync`, which times out against this CMEK bucket —
    # see #93765 and the script's own header for the details.
    step {
      name       = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"
      entrypoint = "bash"
      args = [
        "recidiviz/tools/deploy/sync_repo_to_colab_mirror.sh",
        "gs://recidiviz-staging-repositories/pulse-data",
      ]
      id = "sync-working-tree-to-repositories-bucket"
    }

    # Generous relative to the ~10 minutes the upload of ~26.5k objects takes at
    # the throughput observed in this builder, so a slow run fails loudly on a
    # real error rather than on the clock.
    timeout = "3600s"
  }
}
