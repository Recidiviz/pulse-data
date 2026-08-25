# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

resource "google_project_service" "bigquery_connection_api" {
  service = "bigqueryconnection.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

# TODO(OBT-44639): remove this block once both projects have applied it.
# The validation_results table is now managed by the source-table framework as a protected
# table (see source_tables/yaml_managed/validation_results/). Deregister the Terraform table
# resource without destroying it, so the framework adopts the existing table in place.
removed {
  from = google_bigquery_table.validation_results
  lifecycle {
    destroy = false
  }
}

# TODO(OBT-44639): remove this block once both projects have applied it.
# The jii_texting_incoming_messages table is now managed by the source-table framework as a
# protected table (see source_tables/yaml_managed/twilio_webhook_requests/). Deregister the
# Terraform table resource without destroying it, so the framework adopts the existing table
# in place.
removed {
  from = google_bigquery_table.jii_texting_incoming_messages
  lifecycle {
    destroy = false
  }
}
