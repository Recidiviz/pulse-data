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

# Postgres Version 13 Upgrade Databases
resource "google_sql_database" "state_v2_primary" {
  name     = "${lower(var.state_code)}_primary"
  instance = var.v2_cloudsql_instance_name
}

resource "google_sql_database" "state_v2_secondary" {
  name     = "${lower(var.state_code)}_secondary"
  instance = var.v2_cloudsql_instance_name
}

resource "google_bigquery_connection" "state_v2_primary_bq_connection" {
  provider = google-beta

  connection_id = "state_v2_${lower(var.state_code)}_primary_cloudsql"
  friendly_name = "${var.state_code} State Cloud SQL Postgres (Primary)"
  location      = var.v2_cloudsql_instance_region
  description   = "Connection to the ${var.state_code} State Cloud SQL database (primary)"

  cloud_sql {
    instance_id = var.v2_cloudsql_instance_id
    database    = google_sql_database.state_v2_primary.name
    type        = "POSTGRES"
    credential {
      username = var.v2_cloudsql_instance_user_name
      password = var.v2_cloudsql_instance_user_password
    }
  }
}

resource "google_bigquery_connection" "state_v2_secondary_bq_connection" {
  provider = google-beta

  connection_id = "state_v2_${lower(var.state_code)}_secondary_cloudsql"
  friendly_name = "${var.state_code} State Cloud SQL Postgres (Secondary)"
  location      = var.v2_cloudsql_instance_region
  description   = "Connection to the ${var.state_code} State Cloud SQL database (secondary)"

  cloud_sql {
    instance_id = var.v2_cloudsql_instance_id
    database    = google_sql_database.state_v2_secondary.name
    type        = "POSTGRES"
    credential {
      username = var.v2_cloudsql_instance_user_name
      password = var.v2_cloudsql_instance_user_password
    }
  }
}
