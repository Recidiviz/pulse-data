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
  direct_ingest_regions_package       = "${local.recidiviz_root}/ingest/direct/regions"
  direct_ingest_region_manifest_paths = fileset(local.direct_ingest_regions_package, "*/manifest.yaml")
  all_direct_ingest_region_manifests = {
    for f in local.direct_ingest_region_manifest_paths : upper(dirname(f)) => yamldecode(file("${local.direct_ingest_regions_package}/${f}"))
  }
  # Only include regions that we want to deploy to this environment.
  # Note: We still create infrastructure in prod for regions that don't yet have ingest
  # enabled there, but we don't create infrastructure in prod for playground regions.
  direct_ingest_region_manifests_to_deploy = {
    for region, manifest in local.all_direct_ingest_region_manifests : region => manifest
    # Skip playground regions if we are in prod.
    if !local.is_production || !lookup(manifest, "playground", false)
  }

  sftp_state_alpha_codes = yamldecode(file("${path.module}/config/sftp_state_alpha_codes.yaml"))
}
