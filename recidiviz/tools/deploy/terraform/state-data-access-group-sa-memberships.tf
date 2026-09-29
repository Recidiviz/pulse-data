# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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

# Standing service-account memberships in the state BQ data access groups,
# declared in big_query/config/state_data_access_group_sa_memberships.yaml
# (see that file for the schema and for what deliberately stays out of it).
#
# Group memberships are global Workspace resources, so exactly one
# environment's Terraform state owns each membership: the `managed_by` field
# in the YAML picks the owner, and each environment expands only its own
# entries. The config files are excluded from the public pulse-data mirror;
# the fileexists fallbacks keep the mirrored `terraform validate` CI job
# working there (see the mirror note in direct-ingest-constants.tf).

locals {
  state_data_access_group_sa_memberships_path = "${local.recidiviz_root}/big_query/config/state_data_access_group_sa_memberships.yaml"
  state_data_access_group_sa_memberships = (
    fileexists(local.state_data_access_group_sa_memberships_path)
    ? yamldecode(file(local.state_data_access_group_sa_memberships_path)).service_accounts
    : {}
  )

  # Resource name for s-big-query-admins@recidiviz.org, whose row policy
  # grants every state's rows. Empty only in the mirror.
  big_query_admins_group_resource_name = lookup(
    local.state_data_access_group_settings,
    "big_query_admins_group_resource_name",
    "",
  )

  # Flatten this environment's (service account, group token) pairs. Keys are
  # plan-known strings. ALL_STATES expands to every state in
  # state_data_access_groups.yaml, so a new state's group receives every
  # ALL_STATES service account at the deploy that creates the group.
  standing_sa_membership_pairs = merge([
    for sa, cfg in local.state_data_access_group_sa_memberships : {
      for token in flatten([
        for t in cfg.memberships :
        t == "ALL_STATES" ? keys(local.state_data_access_group_resource_names) : [t]
      ]) : "${sa}|${token}" => { sa = sa, token = token }
    } if cfg.managed_by == var.project_id
  ]...)
}

resource "google_cloud_identity_group_membership" "standing_sa_membership" {
  for_each = local.standing_sa_membership_pairs

  group = (
    each.value.token == "DEFAULT"
    ? local.default_state_data_group_resource_name
    : each.value.token == "BIG_QUERY_ADMINS"
    ? local.big_query_admins_group_resource_name
    : local.state_data_access_group_resource_names[each.value.token]
  )

  preferred_member_key {
    id = each.value.sa
  }

  roles {
    name = "MEMBER"
  }
}

# TODO(OBT-52513): Delete this block and the import-ids YAML once both
# environments have applied the adoption.
#
# One-time adoption of the memberships that existed before this file,
# captured in the 2026-09-03 audit. Import blocks are idempotent, so keeping
# this across deploys is safe. Pairs with no import id (for example, a new
# state's memberships) are created instead of imported.
locals {
  standing_sa_membership_import_ids_path = "${local.recidiviz_root}/big_query/config/state_data_access_group_sa_membership_import_ids.yaml"
  standing_sa_membership_import_ids = (
    fileexists(local.standing_sa_membership_import_ids_path)
    ? yamldecode(file(local.standing_sa_membership_import_ids_path))
    : {}
  )
}

import {
  for_each = {
    for key, membership_name in local.standing_sa_membership_import_ids :
    key => membership_name
    if contains(keys(local.standing_sa_membership_pairs), key)
  }
  to = google_cloud_identity_group_membership.standing_sa_membership[each.key]
  id = each.value
}
