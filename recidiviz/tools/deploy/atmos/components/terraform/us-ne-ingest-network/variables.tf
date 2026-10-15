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

variable "project_id" {
  type        = string
  description = "Project we are provisioning to"
}

variable "region" {
  type        = string
  description = "Region the ingest subnetworks, jump box and VPN live in"
}

variable "zone" {
  type        = string
  description = "Zone the VPN jump box VM lives in"
}

variable "jump_box_ssh_source_ranges" {
  type        = list(string)
  description = <<-EOT
    Source CIDR ranges allowed to open tcp/22 into the VPC by the
    us-ne-allow-ssh rule. Kept as a stack variable so the allowlist can be
    changed without touching this component.
  EOT
}

variable "vpn_shared_secret" {
  type        = string
  sensitive   = true
  description = <<-EOT
    IPsec pre-shared key for the tunnel to Nebraska. Required with no default
    and deliberately not set in the stack, so it never lands in git. The GCP
    API only ever returns this value's hash, so Terraform cannot learn the real
    one; google_compute_vpn_tunnel.ne_tunnel therefore ignores changes to it and
    whatever is passed here is never sent to the API. Supply any non-empty
    placeholder in the environment at plan time, as a TF_VAR_ entry named after
    this variable.
  EOT
}

variable "vpn_peer_ip" {
  type        = string
  sensitive   = true
  description = <<-EOT
    Public IP of Nebraska's end of the IPsec tunnel. Required with no default
    and never set in the stack: this repo already keeps Nebraska's addresses in
    Secret Manager rather than in code (us_ne_vpn_jumpbox_ip,
    us_ne_sql_server_ip), so a partner endpoint does not belong in git either.
    Read it from the live tunnel and pass it at plan time.
  EOT
}

variable "vpn_peer_routed_cidr_range" {
  type        = string
  description = <<-EOT
    Nebraska range routed over the tunnel and allowed to reach SQL Server on
    tcp/1433. Required with no default, for the same reason as vpn_peer_ip.
  EOT
}

variable "vpn_peer_icmp_cidr_range" {
  type        = string
  description = <<-EOT
    Wider Nebraska range allowed to ping into the VPC. Required with no
    default, for the same reason as vpn_peer_ip.
  EOT
}
