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
resource "google_redis_instance" "data_discovery_cache" {
  name           = "data-discovery-cache"
  region         = var.app_engine_region
  memory_size_gb = 8
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}

resource "google_vpc_access_connector" "redis_vpc_connector" {
  name          = "redis-vpc-connector"
  region        = var.app_engine_region
  ip_cidr_range = "10.8.0.0/28"
  network       = "default"
}
