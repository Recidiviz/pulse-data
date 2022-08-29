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

terraform {
  experiments = [module_variable_optional_attrs]
}

# The project for the related database instance
variable "project_id" {
  type = string
}

# The a string key for the database instance, e.g. "state" or "justice_counts".
variable "instance_key" {
  type = string
}

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


variable "additional_databases" {
  type = set(string)
  default = []
}

variable "insights_config" {
  type = object({
    query_insights_enabled  = optional(bool)
    query_string_length     = optional(number)
    record_application_tags = optional(bool)
    record_client_address   = optional(bool)
  })
  default = null
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

  database_friendly_name = title(replace(var.instance_key, "_", " "))

  bq_connection_friendly_name = var.instance_key == "state" ? "LEGACY ${local.database_friendly_name}" : local.database_friendly_name
}


resource "google_sql_database_instance" "data" {
  name                = local.stripped_cloudsql_instance_id
  database_version    = var.database_version
  region              = var.region
  deletion_protection = false

  settings {
    disk_autoresize = true
    tier            = var.tier
    availability_type = "REGIONAL"

    backup_configuration {
      enabled  = true
      location = "us"
      point_in_time_recovery_enabled = true
    }
    
    database_flags {
      name  = "log_checkpoints"
      value = "on"
    }

    database_flags {
      name  = "log_connections"
      value = "on"
    }
    
    database_flags {
      name  = "log_disconnections"
      value = "on"
    }
    
    database_flags {
      name  = "log_duration"
      value = "on"
    }
    
    database_flags {
      name  = "log_lock_waits"
      value = "on"
    }
    
    database_flags {
      name  = "log_statement"
      value = "on"
    }
    
    database_flags {
      name  = "log_hostname"
      value = "on"
    }
    
    database_flags {
      name  = "log_min_messages"
      value = "on"
    }
    
    database_flags {
      name  = "log_min_error_statement"
      value = "on"
    }
    
    database_flags {
      name  = "log_temp_files"
      value = "on"
    }
    
    database_flags {
      name  = "log_min_duration_statement"
      value = "on"
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

    dynamic insights_config {
      # The var.insights_config[*] syntax is a special mode of the splat operator [*]
      # when applied to a non-list value: if var.insights_config is null then it will
      # produce an empty list, and otherwise it will produce a single-element list
      # containing the value.
      for_each = var.insights_config[*]

      content {
        query_insights_enabled  = var.insights_config.query_insights_enabled
        query_string_length     = var.insights_config.query_string_length
        record_application_tags = var.insights_config.record_application_tags
        record_client_address   = var.insights_config.record_client_address
      }
    }

  }
}

resource "google_project_iam_member" "gcs-read-write-access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_sql_database_instance.data.service_account_email_address}"
}

resource "google_sql_user" "postgres" {
  instance = google_sql_database_instance.data.name
  name     = data.google_secret_manager_secret_version.db_user.secret_data
  password = data.google_secret_manager_secret_version.db_password.secret_data
}

resource "google_sql_user" "readonly" {
  instance = google_sql_database_instance.data.name
  name     = length(data.google_secret_manager_secret_version.db_readonly_user) > 0 ? data.google_secret_manager_secret_version.db_readonly_user[0].secret_data : null
  password = length(data.google_secret_manager_secret_version.db_readonly_password) > 0 ? data.google_secret_manager_secret_version.db_readonly_password[0].secret_data : null

  count = var.has_readonly_user ? 1 : 0
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

# Provides a BQ connection to the default 'postgres' database in
# this instance.
resource "google_bigquery_connection" "default_db_bq_connection" {
  provider = google-beta

  connection_id = "${var.instance_key}_cloudsql"
  friendly_name = "${local.bq_connection_friendly_name} Cloud SQL Postgres"
  # TODO(#7285): Migrate Justice Counts connection to be in same region as instance
  location    = var.instance_key == "justice_counts" ? "us" : var.region
  description = "Connection to the ${local.bq_connection_friendly_name} Cloud SQL database"

  cloud_sql {
    instance_id = data.google_secret_manager_secret_version.cloudsql_instance_id.secret_data
    database    = "postgres"
    type        = "POSTGRES"
    credential {
      username = google_sql_user.postgres.name
      password = google_sql_user.postgres.password
    }
  }
}


resource "google_sql_database" "databases" {
  for_each = var.additional_databases
  name     = each.value
  instance = google_sql_database_instance.data.name
}


output "dbusername" {
  value = var.has_readonly_user ? data.google_secret_manager_secret_version.db_readonly_user[0].secret_data : null
}
output "dbuserpassword" {
  value = var.has_readonly_user ? data.google_secret_manager_secret_version.db_readonly_password[0].secret_data : null
}
