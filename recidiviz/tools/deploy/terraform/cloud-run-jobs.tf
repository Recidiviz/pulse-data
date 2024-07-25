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
resource "google_cloud_run_v2_job" "admin_panel_hydrate_cache" {
  name     = "admin-panel-hydrate-cache"
  location = var.region
  provider = google-beta

  template {
    template {
      containers {
        image   = "us-docker.pkg.dev/${var.registry_project_id}/appengine/default:${var.docker_image_tag}"
        command = ["pipenv"]
        args    = ["run", "python", "-m", "recidiviz.admin_panel.entrypoints.hydrate_cache"]
        env {
          name  = "RECIDIVIZ_ENV"
          value = var.project_id == "recidiviz-123" ? "production" : "staging"
        }
        # This is the expected path for Cloud SQL Unix sockets in most, if not all, GCP runtimes
        # as defined here: https://cloud.google.com/sql/docs/postgres/connect-run#public-ip-default_1
        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }
      }
      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [module.operations_database_v2.connection_name]
        }
      }
      vpc_access {
        connector = google_vpc_access_connector.us_central_redis_vpc_connector.id
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }
  }


  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}
