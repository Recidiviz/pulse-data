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
  region         = var.app_engine_region
  ip_cidr_range  = data.google_secret_manager_secret_version.vpc_access_connector_us_east_cidr.secret_data
  network        = "default"
  max_throughput = 1000
}

# VPC Connector is required for Cloud Run to connect to resources that are in a VPC network
# (such as Redis and Cloud NAT)
resource "google_vpc_access_connector" "us_central_redis_vpc_connector" {
  name           = "us-central-redis-vpc-ac" # Name has a 23 character limit
  region         = var.region
  ip_cidr_range  = data.google_secret_manager_secret_version.vpc_access_connector_us_central_cidr.secret_data
  network        = "default"
  max_throughput = 1000
}

resource "google_compute_address" "external_system_outbound_requests" {
  name         = "external-system-outbound-requests"
  address_type = "EXTERNAL"
  description  = "Static IP for making requests directly to external (state-owned) infrastructure."
  region       = var.region
}

# The names of the following NATs and Routers all say "dataflow", but they're actually for all
# resources in the "default" network.
module "nat_us_central1" {
  source = "./modules/nat"

  nat_name    = "${local.nat_prefix}-dataflow-nat-${var.project_id == "recidiviz-123" ? "us-" : ""}central1"
  router_name = "${local.nat_prefix}-dataflow-nat-router-${var.project_id == "recidiviz-123" ? "us-" : ""}central1"
  region      = "us-central1"
  nat_ips     = [google_compute_address.external_system_outbound_requests.self_link]
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
