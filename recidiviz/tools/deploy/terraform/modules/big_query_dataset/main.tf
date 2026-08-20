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
variable "labels" {
  type    = map(string)
  default = {}
}

variable "location" {
  type    = string
  default = "US"
}

variable "dataset_id" {
  type = string
}

variable "description" {
  type = string
}

variable "default_table_expiration_ms" {
  type    = number
  default = null
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_id
  location                    = var.location
  labels                      = merge(var.labels, { managed_by_terraform = "true" })
  description                 = var.description
  max_time_travel_hours       = "168"
  default_table_expiration_ms = var.default_table_expiration_ms

  lifecycle {
    # The `protection` Resource Manager tag is stamped onto protected-tier datasets
    # out-of-band by the calculation DAG's ApplyDatasetProtectionTagsEntrypoint; it
    # gates the catastrophic-delete deny policy (see
    # recidiviz/tools/deploy/deletion_protection/). This module does not set
    # resource_tags, and the provider treats an unset value as "make it empty", so
    # without this Terraform tries to delete that tag on every apply -- which 403s
    # (the deploy SA has no resourcemanager.tagValueBindings.delete on the tag value)
    # and, if it were permitted, would strip protection right after each deploy. The
    # reconcile job is the sole owner of resource_tags, so Terraform leaves it alone.
    ignore_changes = [resource_tags]
  }
}
