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

# The base name for our database-related secrets per `recidiviz.persistence.database.sqlalchemy_engine_manager`
variable "base_secret_name" {
  type = string
}

# Postgres database version
# See also https://cloud.google.com/sql/docs/postgres/create-instance#create-2nd-gen
variable "database_version" {
  type    = string
  default = "POSTGRES_13"
}

# If true, a readonly user will be created from the configured `readonly` secrets
variable "has_readonly_user" {
  type = bool
}

# Note: Enabling or disabling point-in-time recovery causes the Cloud SQL instance to restart.
variable "point_in_time_recovery_enabled" {
  type    = bool
  default = false
}

# Preferred region for the instance
variable "region" {
  type = string
}

# Require SSL connections?
variable "require_ssl_connection" {
  type    = bool
  default = true
}

# Preferred vCPU/Memory tier for the instance
# See also https://cloud.google.com/sql/docs/postgres/create-instance#machine-types
variable "tier" {
  type = string
}

# Preferred availability zone for the instance
variable "zone" {
  type = string
}


# Used for allowing access from `prod-data-client` to the CloudSQL instance
data "google_secret_manager_secret_version" "prod_data_client_cidr" { secret = "prod_data_client_cidr" }

# Data from Google Secrets Manager is added in this terraform module's scope
# The following secrets are used to configure the instance:
# =========================================================
# Instance ID i.e. `recidiviz-staging:us-east1:dev-case-triage-data`
data "google_secret_manager_secret_version" "cloudsql_instance_id" { secret = "${var.base_secret_name}_cloudsql_instance_id" }

# Default username
data "google_secret_manager_secret_version" "db_user" { secret = "${var.base_secret_name}_db_user" }

# Password for the default user
data "google_secret_manager_secret_version" "db_password" { secret = "${var.base_secret_name}_db_password" }

# (Optional) Readonly user name
data "google_secret_manager_secret_version" "db_readonly_user" {
  secret = "${var.base_secret_name}_db_readonly_user"
  count  = var.has_readonly_user ? 1 : 0
}

# (Optional) Readonly user password
data "google_secret_manager_secret_version" "db_readonly_password" {
  secret = "${var.base_secret_name}_db_readonly_password"
  count  = var.has_readonly_user ? 1 : 0
}

locals {
  split_cloudsql_instance_id = split(":", data.google_secret_manager_secret_version.cloudsql_instance_id.secret_data)

  # Retrieve the last element in the resource identifier
  stripped_cloudsql_instance_id = element(local.split_cloudsql_instance_id, length(local.split_cloudsql_instance_id) - 1)
}


resource "google_sql_database_instance" "data" {
  name             = local.stripped_cloudsql_instance_id
  database_version = var.database_version
  region           = var.region

  settings {
    disk_autoresize = true
    tier            = var.tier

    backup_configuration {
      enabled                        = true
      location                       = "us"
      point_in_time_recovery_enabled = var.point_in_time_recovery_enabled
    }

    ip_configuration {
      ipv4_enabled = true
      require_ssl  = var.require_ssl_connection

      authorized_networks {
        name  = "prod-data-client"
        value = data.google_secret_manager_secret_version.prod_data_client_cidr.secret_data
      }
    }

    location_preference {
      zone = var.zone
    }

    maintenance_window {
      day  = 1
      hour = 0
    }
  }
}

resource "google_project_iam_member" "gcs-access" {
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_sql_database_instance.data.service_account_email_address}"
}

resource "google_sql_user" "postgres" {
  instance = google_sql_database_instance.data.name
  name     = data.google_secret_manager_secret_version.db_user.secret_data
  password = data.google_secret_manager_secret_version.db_password.secret_data
}

resource "google_sql_user" "readonly" {
  instance = google_sql_database_instance.data.name
  name     = each.key
  password = each.value

  # Do not create a readonly user if no secret is configured
  for_each = zipmap(
    data.google_secret_manager_secret_version.db_readonly_user[*].secret_data,
    data.google_secret_manager_secret_version.db_readonly_password[*].secret_data
  )
}


# Create a client certificate for the `prod-data-client`
resource "google_sql_ssl_cert" "client_cert" {
  instance    = google_sql_database_instance.data.name
  common_name = "prod-data-client-${local.stripped_cloudsql_instance_id}"
}

# Store client key in a secret
resource "google_secret_manager_secret" "secret_client_key" {
  secret_id = "${var.base_secret_name}_db_client_key"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "secret_version_client_key" {
  secret      = google_secret_manager_secret.secret_client_key.name
  secret_data = google_sql_ssl_cert.client_cert.private_key
}

# Store client certificate in a secret
resource "google_secret_manager_secret" "secret_client_cert" {
  secret_id = "${var.base_secret_name}_db_client_cert"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "secret_version_client_cert" {
  secret      = google_secret_manager_secret.secret_client_cert.name
  secret_data = google_sql_ssl_cert.client_cert.cert
}

# Store server certificate in a secret
resource "google_secret_manager_secret" "secret_server_cert" {
  secret_id = "${var.base_secret_name}_db_server_cert"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "secret_version_server_cert" {
  secret      = google_secret_manager_secret.secret_server_cert.name
  secret_data = google_sql_database_instance.data.server_ca_cert[0].cert
}
