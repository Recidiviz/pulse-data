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

data "google_secret_manager_secret_version" "vpc_access_connector_us_central_cidr" { secret = "vpc_access_connector_us_central_cidr" }

data "google_secret_manager_secret_version" "vpc_access_connector_us_east_cidr" { secret = "vpc_access_connector_us_east_cidr" }

resource "google_redis_instance" "data_discovery_cache" {
  name           = "data-discovery-cache"
  region         = var.app_engine_region
  memory_size_gb = 8
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}


resource "google_redis_instance" "case_triage_rate_limiter_cache" {
  name           = "rate-limit-cache"
  region         = var.app_engine_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}


resource "google_redis_instance" "case_triage_sessions_cache" {
  name           = "case-triage-sessions-cache"
  region         = var.app_engine_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}

resource "google_project_service" "vpc_access_connector" {
  service = "vpcaccess.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

# VPC Connector is required for app engine to connect to Redis
resource "google_vpc_access_connector" "redis_vpc_connector" {
  name           = "redis-vpc-connector"
  region         = var.app_engine_region
  ip_cidr_range  = data.google_secret_manager_secret_version.vpc_access_connector_us_east_cidr.secret_data
  network        = "default"
  max_throughput = 1000
}

# VPC Connector is required for Cloud Run to connect to Redis
resource "google_vpc_access_connector" "us_central_redis_vpc_connector" {
  name           = "us-central-redis-vpc-ac" # Name has a 23 character limit
  region         = var.region
  ip_cidr_range  = data.google_secret_manager_secret_version.vpc_access_connector_us_central_cidr.secret_data
  network        = "default"
  max_throughput = 1000
}


# Store host in a secret
resource "google_secret_manager_secret" "data_discovery_redis_host" {
  secret_id = "data_discovery_redis_host"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "secret_version_redis_host" {
  secret      = google_secret_manager_secret.data_discovery_redis_host.name
  secret_data = google_redis_instance.data_discovery_cache.host
}

resource "google_secret_manager_secret" "case_triage_rate_limiter_redis_host" {
  secret_id = "case_triage_rate_limiter_redis_host"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "case_triage_rate_limiter_redis_host" {
  secret      = google_secret_manager_secret.case_triage_rate_limiter_redis_host.name
  secret_data = google_redis_instance.case_triage_rate_limiter_cache.host
}

resource "google_secret_manager_secret" "case_triage_sessions_redis_host" {
  secret_id = "case_triage_sessions_redis_host"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "case_triage_sessions_redis_host" {
  secret      = google_secret_manager_secret.case_triage_sessions_redis_host.name
  secret_data = google_redis_instance.case_triage_rate_limiter_cache.host
}

# Store port in a secret
resource "google_secret_manager_secret" "data_discovery_redis_port" {
  secret_id = "data_discovery_redis_port"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "data_discovery_redis_port" {
  secret      = google_secret_manager_secret.data_discovery_redis_port.name
  secret_data = google_redis_instance.data_discovery_cache.port
}

resource "google_secret_manager_secret" "case_triage_rate_limiter_redis_port" {
  secret_id = "case_triage_rate_limiter_redis_port"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "case_triage_rate_limiter_redis_port" {
  secret      = google_secret_manager_secret.case_triage_rate_limiter_redis_port.name
  secret_data = google_redis_instance.case_triage_rate_limiter_cache.port
}


resource "google_secret_manager_secret" "case_triage_sessions_redis_port" {
  secret_id = "case_triage_sessions_redis_port"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "case_triage_sessions_redis_port" {
  secret      = google_secret_manager_secret.case_triage_sessions_redis_port.name
  secret_data = google_redis_instance.case_triage_sessions_cache.port
}
