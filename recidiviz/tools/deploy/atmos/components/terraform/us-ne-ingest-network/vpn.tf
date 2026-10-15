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

# The Classic VPN to Nebraska: a reserved external IP, a target VPN gateway,
# the three IPsec forwarding rules, the tunnel, and the route that sends
# Nebraska-bound traffic into it. The tunnel is ESTABLISHED and carries the
# nightly raw data export, so nothing here may plan as a replacement.

resource "google_compute_address" "vpn_ip" {
  name    = "recidiviz-ingest-us-ne-vpn-ip-address"
  project = var.project_id
  region  = var.region

  address_type = "EXTERNAL"
  network_tier = "PREMIUM"
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/addresses/recidiviz-ingest-us-ne-vpn-ip-address"
  to = google_compute_address.vpn_ip
}

resource "google_compute_vpn_gateway" "ne_gateway" {
  name    = "recidiviz-ingest-us-ne-vpn-gateway"
  project = var.project_id
  region  = var.region
  network = google_compute_network.ingest_vpc.id
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/targetVpnGateways/recidiviz-ingest-us-ne-vpn-gateway"
  to = google_compute_vpn_gateway.ne_gateway
}

# Classic VPN needs one forwarding rule per IPsec protocol pointed at the
# gateway. Each is a real resource with its own state and its own import id.
resource "google_compute_forwarding_rule" "vpn_esp" {
  name    = "recidiviz-ingest-us-ne-vpn-gateway-rule-esp"
  project = var.project_id
  region  = var.region

  ip_protocol = "ESP"
  ip_address  = google_compute_address.vpn_ip.address
  target      = google_compute_vpn_gateway.ne_gateway.id
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/forwardingRules/recidiviz-ingest-us-ne-vpn-gateway-rule-esp"
  to = google_compute_forwarding_rule.vpn_esp
}

resource "google_compute_forwarding_rule" "vpn_udp500" {
  name    = "recidiviz-ingest-us-ne-vpn-gateway-rule-udp500"
  project = var.project_id
  region  = var.region

  ip_protocol = "UDP"
  port_range  = "500-500"
  ip_address  = google_compute_address.vpn_ip.address
  target      = google_compute_vpn_gateway.ne_gateway.id
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/forwardingRules/recidiviz-ingest-us-ne-vpn-gateway-rule-udp500"
  to = google_compute_forwarding_rule.vpn_udp500
}

resource "google_compute_forwarding_rule" "vpn_udp4500" {
  name    = "recidiviz-ingest-us-ne-vpn-gateway-rule-udp4500"
  project = var.project_id
  region  = var.region

  ip_protocol = "UDP"
  port_range  = "4500-4500"
  ip_address  = google_compute_address.vpn_ip.address
  target      = google_compute_vpn_gateway.ne_gateway.id
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/forwardingRules/recidiviz-ingest-us-ne-vpn-gateway-rule-udp4500"
  to = google_compute_forwarding_rule.vpn_udp4500
}

# Nebraska's end of the tunnel, as an external gateway resource. The Classic
# tunnel below addresses the peer by IP and does not reference this, so it is
# unused today; it pairs with the peerless HA cloud router in network.tf. See
# the PR body.
resource "google_compute_external_vpn_gateway" "ne_vpn_gateway" {
  name    = "ne-vpn-gateway"
  project = var.project_id

  redundancy_type = "SINGLE_IP_INTERNALLY_REDUNDANT"

  interface {
    id         = 0
    ip_address = var.vpn_peer_ip
  }
}

import {
  id = "projects/${var.project_id}/global/externalVpnGateways/ne-vpn-gateway"
  to = google_compute_external_vpn_gateway.ne_vpn_gateway
}

# The pre-shared key is the one value here that cannot be read back out of GCP:
# compute.vpnTunnels.get returns sharedSecretHash, never sharedSecret. Terraform
# imports the tunnel with an empty shared_secret in state, and the attribute is
# ForceNew, so any value in config would plan a destroy-and-recreate of an
# ESTABLISHED tunnel. ignore_changes is what keeps the plan a no-op and means
# var.vpn_shared_secret is never sent to the API; prevent_destroy is the backstop
# if someone removes it.
resource "google_compute_vpn_tunnel" "ne_tunnel" {
  name    = "recidiviz-ingest-us-ne-vpn-gateway-tunnel1"
  project = var.project_id
  region  = var.region

  target_vpn_gateway = google_compute_vpn_gateway.ne_gateway.id
  peer_ip            = var.vpn_peer_ip
  ike_version        = 2
  shared_secret      = var.vpn_shared_secret

  local_traffic_selector  = ["0.0.0.0/0"]
  remote_traffic_selector = ["0.0.0.0/0"]

  depends_on = [
    google_compute_forwarding_rule.vpn_esp,
    google_compute_forwarding_rule.vpn_udp500,
    google_compute_forwarding_rule.vpn_udp4500,
  ]

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [shared_secret]
  }
}

import {
  id = "projects/${var.project_id}/regions/${var.region}/vpnTunnels/recidiviz-ingest-us-ne-vpn-gateway-tunnel1"
  to = google_compute_vpn_tunnel.ne_tunnel
}

# Sends the Nebraska range that holds the SQL Server into the tunnel.
resource "google_compute_route" "ne_tunnel_route" {
  name    = "recidiviz-ingest-us-ne-vpn-gateway-tunnel1-route-1"
  project = var.project_id
  network = google_compute_network.ingest_vpc.name

  dest_range          = var.vpn_peer_routed_cidr_range
  priority            = 800
  next_hop_vpn_tunnel = google_compute_vpn_tunnel.ne_tunnel.id
}

import {
  id = "projects/${var.project_id}/global/routes/recidiviz-ingest-us-ne-vpn-gateway-tunnel1-route-1"
  to = google_compute_route.ne_tunnel_route
}
