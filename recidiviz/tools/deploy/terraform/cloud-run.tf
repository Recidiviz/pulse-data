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

resource "google_cloud_run_service" "case-triage" {
  name     = "case-triage-web"
  location = var.region

  template {
    spec {
      containers {
        image   = "us.gcr.io/recidiviz-staging/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "gunicorn", "-c", "gunicorn.conf.py", "--log-file=-", "-b", ":$PORT", "recidiviz.case_triage.server:app"]
      }
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1000"
        "run.googleapis.com/cloudsql-instances" = local.joined_connection_string
      }
      name = "case-triage-web-${var.docker_image_tag}"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = false
}

resource "google_cloud_run_service_iam_member" "public-access" {
  location = google_cloud_run_service.case-triage.location
  project  = google_cloud_run_service.case-triage.project
  service  = google_cloud_run_service.case-triage.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
