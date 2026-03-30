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

  # Cloud Identity group resource names for state-specific BQ data access groups.
  # Loaded from YAML config; see big_query/config/state_data_access_groups.yaml for details
  # on how to add new states.
  state_data_access_group_resource_names = yamldecode(file("${local.recidiviz_root}/big_query/config/state_data_access_groups.yaml"))

  # Resource name for s-default-state-data@recidiviz.org, used for non-restricted
  # states that need access to non-restricted state data in state-agnostic BQ tables.
  default_state_data_group_resource_name = "groups/03cqmetx1ujo2ct"

  # States with row-level access restrictions, loaded from the same YAML that
  # Python's row_access_policy_query_builder.py uses, so the two stay in sync.
  restricted_access_state_groups = yamldecode(file("${local.recidiviz_root}/big_query/config/restricted_access_state_groups.yaml"))
  restricted_access_states       = toset(keys(local.restricted_access_state_groups))
}
