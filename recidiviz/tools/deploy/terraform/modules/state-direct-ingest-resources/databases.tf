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

# Postgres Version 13 Upgrade Databases

# TODO(#20930): Actually delete this state CloudSQL database once we're really sure we don't
# need the legacy ingest data in there anymore.
resource "google_sql_database" "state_v2_primary" {
  name     = "${lower(var.state_code)}_primary"
  instance = var.v2_cloudsql_instance_name
}

# TODO(#20930): Actually delete this state CloudSQL database once we're really sure we don't
# need the legacy ingest data in there anymore.
resource "google_sql_database" "state_v2_secondary" {
  name     = "${lower(var.state_code)}_secondary"
  instance = var.v2_cloudsql_instance_name
}
