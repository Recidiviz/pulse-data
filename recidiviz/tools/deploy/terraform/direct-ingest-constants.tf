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

# States with Terraform-managed infrastructure for direct ingest
locals {
  direct_ingest_regions_package = "${local.recidiviz_root}/ingest/direct/regions"
  direct_ingest_region_manifest_paths = fileset(local.direct_ingest_regions_package, "*/manifest.yaml")
  direct_ingest_region_manifests = {
    for f in local.direct_ingest_region_manifest_paths : upper(dirname(f)) => yamldecode(file("${local.direct_ingest_regions_package}/${f}"))
  }

  direct_ingest_state_manifests = {
    # State codes follow format 'US_XX' so are 5 chars long.
    for region_code, manifest in local.direct_ingest_region_manifests: region_code => manifest if length(region_code) == 5
  }

  sftp_state_alpha_codes = yamldecode(file("${path.module}/config/sftp_state_alpha_codes.yaml"))

  # County codes follow format 'US_XX_YY..' so are longer than 5 chars long.
  direct_ingest_county_manifests = {
    for region_code, manifest in local.direct_ingest_region_manifests: region_code => manifest if length(region_code) > 5
  }
}
