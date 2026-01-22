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

data "google_secret_manager_secret_version" "vpc_access_connector_cf_cidr" { secret = "vpc_access_connector_cf_cidr" }

data "google_secret_manager_secret_version" "us_tx_sftp_host_ip" { secret = "us_tx_sftp_host_ip" }

data "google_secret_manager_secret_version" "us_pa_sftp_host_ip" { secret = "us_pa_sftp_host_ip" }

resource "google_project_service" "vpc_access_connector" {
  service = "vpcaccess.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

resource "google_compute_project_default_network_tier" "project-tier" {
  project      = var.project_id
  network_tier = "PREMIUM"
}


# VPC Connector is required for app engine to connect to resources that are in a VPC network
# (such as Redis)
resource "google_vpc_access_connector" "redis_vpc_connector" {
  name           = "redis-vpc-connector"
  region         = var.us_east_region
  ip_cidr_range  = data.google_secret_manager_secret_version.vpc_access_connector_us_east_cidr.secret_data
  network        = "default"
  max_throughput = 1000
}

# VPC Connector is required for Cloud Run to connect to resources that are in a VPC network
# (such as Redis and Cloud NAT)
resource "google_vpc_access_connector" "us_central_redis_vpc_connector" {
  name           = "us-central-redis-vpc-ac" # Name has a 23 character limit
  region         = var.us_central_region
  ip_cidr_range  = data.google_secret_manager_secret_version.vpc_access_connector_us_central_cidr.secret_data
  network        = "default"
  max_throughput = 1000
}

# The VPC Connector is required to route a Cloud Function's egress traffic through a VPC network.
# This setup ensures a function can securely call another Cloud Function
# while restricting the ingress traffic to internal requests only.
resource "google_vpc_access_connector" "cloud_function_vpc_connector" {
  name          = "cf-vpc-connector"
  region        = "us-central1"
  ip_cidr_range = data.google_secret_manager_secret_version.vpc_access_connector_cf_cidr.secret_data
  network       = "default"
  # Only one of max_throughput and max_instances can be specified and
  # the use of max_throughput is discouraged in favor of max_instances according to documentation
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/vpc_access_connector
  # but only specifying max_instances causes the vpc connector to be recreated on every deploy
  max_throughput = 1000
}


resource "google_compute_address" "external_system_outbound_requests" {
  name         = "external-system-outbound-requests"
  address_type = "EXTERNAL"
  description  = "Static IP for making requests directly to external (state-owned) infrastructure."
  region       = var.us_central_region
}

# TODO(#37766) rollback once we confirm w/ texas this was an ip-based issue
resource "google_compute_address" "us_tx_sftp_outbound_requests" {
  name         = "us-tx-sftp-temp-outbound-requests"
  address_type = "EXTERNAL"
  description  = "Static IP for making requests directly to Texas. *Should* hopefully a temporary address, see #37766"
  region       = var.us_central_region
}

# TODO(#57660) rollback once we confirm w/ pennsylvania this was an ip-based issue
resource "google_compute_address" "us_pa_sftp_outbound_requests" {
  name         = "us-pa-sftp-temp-outbound-requests"
  address_type = "EXTERNAL"
  description  = "Static IP for making requests directly to Pennsylvania. *Should* hopefully a temporary address, see #57660"
  region       = var.us_central_region
}

# The names of the following NATs and Routers all say "dataflow", but they're actually for all
# resources in the "default" network.
module "nat_us_central1" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat-${var.project_id == "recidiviz-123" ? "us-" : ""}central1"
  router_name = "${local.nat_prefix}-dataflow-nat-router-${var.project_id == "recidiviz-123" ? "us-" : ""}central1"
  region      = "us-central1"
  nat_ips     = [google_compute_address.external_system_outbound_requests.self_link]

  # TODO(#37766) rollback once we confirm w/ texas this was an ip-based issue
  # TODO(#57660) rollback once we confirm w/ pennsylvania this was an ip-based issue

  # Determines whether existing port mappings can be used for the multiple connections
  # from teh same internal to external IP.
  # Specifying rules requires making this value false.
  enable_endpoint_independent_mapping = false
  rules = [
    {
      # rule_number determines the order in which the rules are checked. The lower the
      # number, the higher the priority.
      rule_number = 64998
      description = "Route requests to the US_TX SFTP server through a separate IP"
      match       = "destination.ip == '${data.google_secret_manager_secret_version.us_tx_sftp_host_ip.secret_data}'"
      action = {
        source_nat_active_ips = [google_compute_address.us_tx_sftp_outbound_requests.self_link]
      }
    },
    {
      rule_number = 64999
      description = "Route requests to the US_PA SFTP server through a separate IP"
      match       = "destination.ip == '${data.google_secret_manager_secret_version.us_pa_sftp_host_ip.secret_data}'"
      action = {
        source_nat_active_ips = [google_compute_address.us_pa_sftp_outbound_requests.self_link]
      }
    }
  ]
}

module "nat_us_west1" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat${var.project_id == "recidiviz-123" ? "-us-west1" : ""}"
  router_name = "${local.nat_prefix}-dataflow-nat-router${var.project_id == "recidiviz-123" ? "-us-west-1" : ""}"
  region      = "us-west1"
}

module "nat_us_east1" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat-us-east1"
  router_name = "${local.nat_prefix}-dataflow-nat-router-us-east1"
  region      = "us-east1"
}

module "nat_us_east4" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat-us-east4"
  router_name = "${local.nat_prefix}-dataflow-nat-router-us-east4"
  region      = "us-east4"
}

module "nat_us_west2" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat-us-west2"
  router_name = "${local.nat_prefix}-dataflow-nat-router-us-west2"
  region      = "us-west2"
}

module "nat_us_west3" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat-us-west3"
  router_name = "${local.nat_prefix}-dataflow-nat-router-us-west3"
  region      = "us-west3"
}

locals {
  nat_prefix = var.project_id == "recidiviz-123" ? "recidiviz-production" : var.project_id
}


locals {
  # Private Google Access IPs
  # https://cloud.google.com/vpc/docs/configure-private-google-access#config-domain
  private_google_access_ips = [
    "199.36.153.8",
    "199.36.153.9",
    "199.36.153.10",
    "199.36.153.11",
  ]

  # Domains required for Private IP Cloud Composer
  private_dns_zones = {
    "googleapis" = {
      dns_name    = "googleapis.com."
      description = "Private Google Access for googleapis.com"
    }
    "pkg-dev" = {
      dns_name    = "pkg.dev."
      description = "Private Google Access for pkg.dev"
    }
    "gcr-io" = {
      dns_name    = "gcr.io."
      description = "Private Google Access for gcr.io"
    }
  }
}


data "google_compute_network" "vpc" {
  name    = "default"
  project = var.project_id
}

# Create private DNS zones
resource "google_dns_managed_zone" "private_google_access" {
  for_each = local.private_dns_zones

  project     = var.project_id
  name        = "private-${each.key}"
  dns_name    = each.value.dns_name
  description = each.value.description
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = data.google_compute_network.vpc.id
    }
  }
}

# Create A records pointing to Private Google Access IPs
resource "google_dns_record_set" "private_google_access_a" {
  for_each = local.private_dns_zones

  project      = var.project_id
  managed_zone = google_dns_managed_zone.private_google_access[each.key].name
  name         = each.value.dns_name
  type         = "A"
  ttl          = 300
  rrdatas      = local.private_google_access_ips
}

# Create CNAME records for wildcard subdomains
resource "google_dns_record_set" "private_google_access_cname" {
  for_each = local.private_dns_zones

  project      = var.project_id
  managed_zone = google_dns_managed_zone.private_google_access[each.key].name
  name         = "*.${each.value.dns_name}"
  type         = "CNAME"
  ttl          = 300
  rrdatas      = [each.value.dns_name]
}
