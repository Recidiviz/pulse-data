# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

variable "nat_name" {
  type = string
}

variable "router_name" {
  type = string
}

variable "region" {
  type = string
}

variable "nat_ips" {
  type    = list(string)
  default = []
}

resource "google_compute_router" "default" {
  name        = var.router_name
  region      = var.region
  network     = "default"
  description = "A Cloud Router for NAT in the ${var.region} region."
}

resource "google_compute_router_nat" "default" {
  name   = var.nat_name
  region = var.region
  router = google_compute_router.default.name

  nat_ip_allocate_option             = length(var.nat_ips) == 0 ? "AUTO_ONLY" : "MANUAL_ONLY"
  nat_ips                            = var.nat_ips
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  min_ports_per_vm                   = 64

  log_config {
    enable = true
    filter = "ALL"
  }
}
