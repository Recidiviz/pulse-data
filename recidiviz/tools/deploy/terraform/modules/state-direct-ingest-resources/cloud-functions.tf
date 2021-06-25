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
resource "google_cloudfunctions_function" "primary-ingest" {
  name    = local.direct_ingest_formatted_str
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.direct-ingest-bucket.name
  }

  entry_point = "handle_state_direct_ingest_file"
  environment_variables = {
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = var.repo_url
  }

  timeout = 540
}

resource "google_cloudfunctions_function" "secondary-ingest" {
  name    = "${local.direct_ingest_formatted_str}-secondary"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = module.secondary-direct-ingest-bucket.name
  }

  entry_point = "handle_state_direct_ingest_file"
  environment_variables = {
    "GCP_PROJECT" = var.project_id
  }

  source_repository {
    url = var.repo_url
  }

  timeout = 540
}
