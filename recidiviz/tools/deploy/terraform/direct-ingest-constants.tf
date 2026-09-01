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

  # States that have a document collections config directory
  document_collection_states = toset([
    for region, manifest in local.direct_ingest_region_manifests_to_deploy : region
    if length(fileset("${local.recidiviz_root}/documents/config/document_collections/${lower(region)}", "*.yaml")) > 0
  ])

  sftp_state_alpha_codes = yamldecode(file("${path.module}/config/sftp_state_alpha_codes.yaml"))

  # Cloud Identity groups for state-specific BQ data access, loaded from YAML
  # config; see big_query/config/state_data_access_groups.yaml for details on
  # how to add new states. A state maps either to a pinned groups/{id} resource
  # name (a legacy group, created by hand) or to the TF_MANAGED sentinel (a
  # group that Terraform creates; see state-data-access-groups.tf).
  #
  # The group config files below are excluded from the public pulse-data
  # mirror, while this file and the mirrored CI's `terraform validate` job are
  # not. Each read falls back to an empty config when its file is absent, so
  # validation still evaluates in the mirror. In the private repo, CI fails
  # when any of these files is missing -- see
  # test_private_group_config_files_exist in
  # recidiviz/tests/tools/deploy/terraform/state_data_access_groups_test.py
  # (that test is itself excluded from the mirror).
  state_data_access_groups_config_path = "${local.recidiviz_root}/big_query/config/state_data_access_groups.yaml"
  state_data_access_groups_config = (
    fileexists(local.state_data_access_groups_config_path)
    ? yamldecode(file(local.state_data_access_groups_config_path))
    : {}
  )
  tf_managed_data_access_group_sentinel = "TF_MANAGED"

  # Group settings that are not per-state entries: the default group resource
  # name and the group email format.
  state_data_access_group_settings_path = "${local.recidiviz_root}/big_query/config/state_data_access_group_settings.yaml"
  state_data_access_group_settings = (
    fileexists(local.state_data_access_group_settings_path)
    ? yamldecode(file(local.state_data_access_group_settings_path))
    : {}
  )

  # States whose groups Terraform creates and owns.
  tf_managed_data_access_group_states = toset([
    for state, value in local.state_data_access_groups_config : state
    if value == local.tf_managed_data_access_group_sentinel
  ])

  # Legacy groups, with resource names pinned in the YAML. These groups are
  # not in Terraform state.
  legacy_state_data_access_group_resource_names = {
    for state, value in local.state_data_access_groups_config : state => value
    if value != local.tf_managed_data_access_group_sentinel
  }

  # Resource names for every state's group, however the group is owned. The
  # TF-managed map is defined in state-data-access-groups.tf; its values are
  # not known until apply, so do not use this map for for_each keys.
  state_data_access_group_resource_names = merge(
    local.legacy_state_data_access_group_resource_names,
    local.tf_managed_state_data_access_group_resource_names,
  )

  # Resource name for the shared default-state data access group, used for
  # non-restricted states that need access to non-restricted state data in
  # state-agnostic BQ tables. Empty only in the mirror (see above).
  default_state_data_group_resource_name = lookup(
    local.state_data_access_group_settings,
    "default_state_data_group_resource_name",
    "",
  )

  # States with row-level access restrictions, loaded from the same YAML that
  # Python's row_access_policy_query_builder.py uses, so the two stay in sync.
  restricted_access_state_groups_path = "${local.recidiviz_root}/big_query/config/restricted_access_state_groups.yaml"
  restricted_access_state_groups = (
    fileexists(local.restricted_access_state_groups_path)
    ? yamldecode(file(local.restricted_access_state_groups_path))
    : {}
  )
  restricted_access_states = toset(keys(local.restricted_access_state_groups))
}
