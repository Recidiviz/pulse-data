# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

resource "google_project_service" "bigquery_connection_api" {
  service = "bigqueryconnection.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy = true
}

# TODO(#5082): Once the Cloud SQL instance exists in terraform, use this to provision the connection.
# resource "google_bigquery_connection" "justice_counts_connection" {
#     provider      = google-beta
#     connection_id = "justice_counts_cloudsql"
#     friendly_name = "Justice Counts Cloud SQL Postgres"
#     description   = "Connection to the Justice Counts Cloud SQL database"
#     cloud_sql {
#         instance_id = google_sql_database_instance.instance.connection_name
#         database    = google_sql_database.db.name
#         type        = "POSTGRES"
#         credential {
#           username = google_sql_user.user.name
#           password = google_sql_user.user.password
#         }
#     }
# }
