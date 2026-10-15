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

# The four ingress rules on the ingest VPC. None of them carries a target tag,
# so each applies to every instance in the VPC - which is only the jump box.

# Admits SSH from IAP TCP forwarding only. This was open to the whole internet
# until Security Command Center flagged it.
resource "google_compute_firewall" "allow_ssh" {
  name    = "us-ne-allow-ssh"
  project = var.project_id
  network = google_compute_network.ingest_vpc.id

  direction     = "INGRESS"
  priority      = 1000
  disabled      = false
  source_ranges = var.jump_box_ssh_source_ranges

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}

import {
  id = "projects/${var.project_id}/global/firewalls/us-ne-allow-ssh"
  to = google_compute_firewall.allow_ssh
}

# Traffic between the ingest subnetwork and the Private NAT range. This is what
# lets the Cloud Run export job reach the jump box on tcp/22, independently of
# us-ne-allow-ssh.
resource "google_compute_firewall" "allow_vpc_internal" {
  name    = "us-ne-allow-vpc-internal"
  project = var.project_id
  network = google_compute_network.ingest_vpc.id

  description = "VPC-internal traffic only (replaces dangerous us-ne-allow-internal rule)"
  direction   = "INGRESS"
  priority    = 1000
  disabled    = false

  source_ranges = [
    google_compute_subnetwork.ingest_subnetwork.ip_cidr_range,
    google_compute_subnetwork.private_nat.ip_cidr_range,
  ]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}

import {
  id = "projects/${var.project_id}/global/firewalls/us-ne-allow-vpc-internal"
  to = google_compute_firewall.allow_vpc_internal
}

# SQL Server and ping from Nebraska's range at the far end of the tunnel.
resource "google_compute_firewall" "allow_vpn_traffic" {
  name    = "allow-vpn-traffic"
  project = var.project_id
  network = google_compute_network.ingest_vpc.id

  direction     = "INGRESS"
  priority      = 1000
  disabled      = false
  source_ranges = [var.vpn_peer_routed_cidr_range]

  allow {
    protocol = "tcp"
    ports    = ["1433"]
  }

  allow {
    protocol = "icmp"
  }

  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}

import {
  id = "projects/${var.project_id}/global/firewalls/allow-vpn-traffic"
  to = google_compute_firewall.allow_vpn_traffic
}

# Ping from a wider slice of Nebraska's range than the tunnel route covers.
resource "google_compute_firewall" "allow_icmp_vpn" {
  name    = "allow-icmp-vpn"
  project = var.project_id
  network = google_compute_network.ingest_vpc.id

  direction     = "INGRESS"
  priority      = 1000
  disabled      = false
  source_ranges = [var.vpn_peer_icmp_cidr_range]

  allow {
    protocol = "icmp"
  }

  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}

import {
  id = "projects/${var.project_id}/global/firewalls/allow-icmp-vpn"
  to = google_compute_firewall.allow_icmp_vpn
}
