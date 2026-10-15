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

output "vpc_network_name" {
  description = "Name of the ingest VPC network"
  value       = google_compute_network.ingest_vpc.name
}

output "vpc_subnetwork_name" {
  description = "Name of the ingest VPC subnetwork the Cloud Run export job runs in"
  value       = google_compute_subnetwork.ingest_subnetwork.name
}

output "vpc_subnetwork_ip_cidr_range" {
  description = "Primary CIDR range of the ingest VPC subnetwork"
  value       = google_compute_subnetwork.ingest_subnetwork.ip_cidr_range
}

output "jump_box_internal_ip" {
  description = "Internal IP of the VPN jump box, which the Cloud Run export job SSHes to"
  value       = google_compute_instance.vpn_jump_box.network_interface[0].network_ip
  sensitive   = true
}
