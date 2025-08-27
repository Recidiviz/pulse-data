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

module "external_reference_tables_bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "external-reference-data"
}

module "state_resident_populations_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "state_resident_populations.csv"
}
