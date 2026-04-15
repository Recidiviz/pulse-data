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

# The project for the related database instance
variable "project_id" {
  type = string
}

# The a string key for the database instance, e.g. "state" or "justice_counts".
variable "instance_key" {
  type = string
}

# The base name for our database-related secrets per `recidiviz.persistence.database.sqlalchemy_engine_manager`
# Defaults to instance_key if not provided.
variable "base_secret_name" {
  type    = string
  default = null
}

# Postgres database version
# See also https://cloud.google.com/sql/docs/postgres/create-instance#create-2nd-gen
variable "database_version" {
  type    = string
  default = "POSTGRES_18"
}

# Cloud SQL edition
# See also https://cloud.google.com/sql/docs/postgres/editions-intro
variable "edition" {
  type    = string
  default = "ENTERPRISE"
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

variable "secondary_zone" {
  type    = string
  default = null
}

variable "encryption_key_name" {
  type        = string
  description = "The full resource name of the Cloud KMS key to use for CMEK encryption."
  default     = null
}

variable "additional_databases" {
  type    = set(string)
  default = []
}

variable "instance_name" {
  type        = string
  description = "The Cloud SQL instance name (not the full connection string). When set, overrides the name derived from instance_key."
  default     = null
}

variable "insights_config" {
  type = object({
    query_insights_enabled  = optional(bool)
    query_string_length     = optional(number)
    record_application_tags = optional(bool)
    record_client_address   = optional(bool)
  })
  default = {
    query_insights_enabled  = true
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}

# Default username
data "google_secret_manager_secret_version" "db_user" { secret = "${local.effective_base_secret_name}_db_user" }

# Password for the default user
data "google_secret_manager_secret_version" "db_password" { secret = "${local.effective_base_secret_name}_db_password" }


locals {
  env_prefix                 = var.project_id == "recidiviz-staging" ? "dev" : "prod"
  effective_base_secret_name = coalesce(var.base_secret_name, var.instance_key)

  effective_instance_name = coalesce(var.instance_name, "${local.env_prefix}-${replace(var.instance_key, "_", "-")}-data")
  connection_name        = "${var.project_id}:${var.region}:${local.effective_instance_name}"

  database_friendly_name = title(replace(var.instance_key, "_", " "))

  bq_connection_friendly_name = var.instance_key == "state" ? "LEGACY ${local.database_friendly_name}" : local.database_friendly_name
}


resource "google_sql_database_instance" "data" {
  name                = local.effective_instance_name
  encryption_key_name = var.encryption_key_name
  database_version    = var.database_version
  region              = var.region
  deletion_protection = false

  settings {
    edition           = var.edition
    disk_autoresize   = true
    tier              = var.tier
    availability_type = "REGIONAL"

    backup_configuration {
      enabled                        = true
      location                       = "us"
      point_in_time_recovery_enabled = true
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
      value = "all"
    }

    database_flags {
      name  = "log_hostname"
      value = "on"
    }

    database_flags {
      name  = "log_min_messages"
      value = "info"
    }


    database_flags {
      name  = "log_min_duration_statement"
      value = 0
    }

    ip_configuration {
      ipv4_enabled = true
      ssl_mode  = var.require_ssl_connection ? "TRUSTED_CLIENT_CERTIFICATE_REQUIRED" : "ALLOW_UNENCRYPTED_AND_ENCRYPTED"

    }

    location_preference {
      zone           = var.zone
      secondary_zone = var.secondary_zone
    }

    maintenance_window {
      day  = 1
      hour = 0
    }

    dynamic "insights_config" {
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
    instance_id = local.connection_name
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
