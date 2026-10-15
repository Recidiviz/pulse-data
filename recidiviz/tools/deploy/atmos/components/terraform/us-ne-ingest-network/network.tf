# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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

# The ingest VPC, its two subnetworks, its Cloud Routers and its non-generated
# routes. All of this already exists in recidiviz-ingest-us-ne and was created
# by hand; every resource is paired with an `import` block so the first plan is
# a pure import with no create, update or replace. Remove the `import` blocks
# after the first successful apply.
#
# Values come from a Cloud Asset Inventory export, not from a live compute read.

resource "google_compute_network" "ingest_vpc" {
  name    = "recidiviz-ingest-us-ne-vpc"
  project = var.project_id

  description                               = "recidiviz-ingest-us-ne"
  auto_create_subnetworks                   = false
  routing_mode                              = "REGIONAL"
  mtu                                       = 1460
  network_firewall_policy_enforcement_order = "AFTER_CLASSIC_FIREWALL"

  # delete_default_routes_on_create is deliberately unset. It is create-time
  # only, ForceNew, and not returned by any read, so it lands as false in
  # imported state; declaring true here would plan a replacement of the VPC.
}

import {
  id = "projects/${var.project_id}/global/networks/recidiviz-ingest-us-ne-vpc"
  to = google_compute_network.ingest_vpc
}

# The subnetwork the jump box sits in and the Cloud Run export job attaches to
# with Direct VPC egress.
resource "google_compute_subnetwork" "ingest_subnetwork" {
  name    = "recidiviz-ingest-us-ne-us-central-1"
  project = var.project_id
  region  = var.region
  network = google_compute_network.ingest_vpc.id

  ip_cidr_range            = "10.0.0.0/9"
  private_ip_google_access = true
  purpose                  = "PRIVATE"
  stack_type               = "IPV4_ONLY"

  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/subnetworks/recidiviz-ingest-us-ne-us-central-1"
  to = google_compute_subnetwork.ingest_subnetwork
}

# Source range Private NAT draws from when translating traffic onto the tunnel.
# A PRIVATE_NAT subnetwork carries no instances, so it has no flow logs and no
# Private Google Access.
resource "google_compute_subnetwork" "private_nat" {
  name    = "private-nat"
  project = var.project_id
  region  = var.region
  network = google_compute_network.ingest_vpc.id

  ip_cidr_range = "10.180.0.0/20"
  purpose       = "PRIVATE_NAT"
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/subnetworks/private-nat"
  to = google_compute_subnetwork.private_nat
}

# Router carrying the Private NAT gateway that maps VM traffic onto the tunnel.
resource "google_compute_router" "private_nat_router" {
  name    = "private-nat-gateway-router-ne"
  project = var.project_id
  region  = var.region
  network = google_compute_network.ingest_vpc.id
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/routers/private-nat-gateway-router-ne"
  to = google_compute_router.private_nat_router
}

resource "google_compute_router_nat" "private_nat_gateway" {
  name    = "private-nat-gateway-ne"
  project = var.project_id
  region  = var.region
  router  = google_compute_router.private_nat_router.name

  type                                = "PRIVATE"
  source_subnetwork_ip_ranges_to_nat  = "LIST_OF_SUBNETWORKS"
  enable_dynamic_port_allocation      = true
  enable_endpoint_independent_mapping = false
  endpoint_types                      = ["ENDPOINT_TYPE_VM"]

  subnetwork {
    name                    = google_compute_subnetwork.ingest_subnetwork.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  rules {
    rule_number = 500
    match       = "nexthop.is_hybrid"

    action {
      source_nat_active_ranges = [google_compute_subnetwork.private_nat.id]
    }
  }

  log_config {
    enable = true
    filter = "ALL"
  }
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/routers/private-nat-gateway-router-ne/private-nat-gateway-ne"
  to = google_compute_router_nat.private_nat_gateway
}

# This router has an ASN but no interfaces and no BGP peers, so it currently
# carries no traffic. It looks like the start of an HA VPN that was never
# finished - the tunnel in vpn.tf is a Classic VPN and does not use it. It is
# imported so it is at least visible in review; see the PR body.
resource "google_compute_router" "ha_cloud_router" {
  name    = "recidiviz-ingest-us-ne-vpc-ha-cloud-router"
  project = var.project_id
  region  = var.region
  network = google_compute_network.ingest_vpc.id

  bgp {
    asn                = 64512
    advertise_mode     = "DEFAULT"
    keepalive_interval = 20
  }
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/routers/recidiviz-ingest-us-ne-vpc-ha-cloud-router"
  to = google_compute_router.ha_cloud_router
}

# Hand-created egress route. The auto-generated internet route the VPC would
# normally carry is absent, so this one is what gives the subnetworks a default
# route out. Subnetwork routes (default-route-r-*) are generated by GCP and are
# not importable, so they are not declared here.
resource "google_compute_route" "default_internet" {
  name    = "default-internet-route"
  project = var.project_id
  network = google_compute_network.ingest_vpc.name

  dest_range       = "0.0.0.0/0"
  priority         = 1000
  next_hop_gateway = "default-internet-gateway"
}

import {
  id = "projects/${var.project_id}/global/routes/default-internet-route"
  to = google_compute_route.default_internet
}
