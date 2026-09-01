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

# Terraform-managed Google Groups for state-specific BQ data access.
#
# Groups live in Google Workspace, which has no staging/prod split: there is
# exactly one s-{state}-data@recidiviz.org group per state. Terraform runs
# once per project with separate state, so exactly one environment must own
# each group resource. Staging owns them, because every deploy reaches staging
# before prod (see recidiviz/tools/deploy/CLAUDE.md). By the time a prod apply
# needs a group, the staging apply has already created it. Prod resolves the
# same groups read-only, via the group lookup data source below.
#
# The CI/CD service account can create and manage groups because it holds a
# Workspace admin role for groups. That role is assigned by hand at
# admin.google.com (Admin roles); it is not in GCP IAM and not in Terraform.
#
# Groups created by hand before this file existed ("legacy" groups) are NOT
# imported into Terraform state. Their resource names stay pinned in
# big_query/config/state_data_access_groups.yaml. See that file for how to
# add a new state.

locals {
  # recidiviz.org Workspace customer. Group resources hang off the customer,
  # not off a GCP project. The ID lives in organization-settings.yaml, which is
  # excluded from the public pulse-data mirror; the fallback keeps `terraform
  # validate` working there (see the mirror note in direct-ingest-constants.tf).
  organization_settings_path = "${local.recidiviz_root}/tools/deploy/atmos/catalogs/organization-settings.yaml"
  workspace_customer_id = (
    fileexists(local.organization_settings_path)
    ? format(
      "customers/%s",
      yamldecode(
        file(local.organization_settings_path)
      ).settings.context.workspace_customer_id,
    )
    : "customers/UNKNOWN"
  )

  # The group email derives from the state code via the format string in the
  # group settings file, which is also excluded from the mirror.
  tf_managed_state_data_access_group_emails = {
    for state in local.tf_managed_data_access_group_states :
    state => replace(
      lookup(
        local.state_data_access_group_settings,
        "state_data_group_email_format",
        "unknown-{state}@example.com",
      ),
      "{state}",
      lower(trimprefix(state, "US_")),
    )
  }

  # groups/{group_id} resource names for TF-managed groups: from the owned
  # resource in staging, from the read-only lookup in prod. Values are only
  # known at apply time, so do not use this map for for_each keys.
  tf_managed_state_data_access_group_resource_names = local.is_production ? {
    for state in local.tf_managed_data_access_group_states :
    state => data.google_cloud_identity_group_lookup.state_data_access_group[state].name
    } : {
    for state in local.tf_managed_data_access_group_states :
    state => google_cloud_identity_group.state_data_access_group[state].name
  }
}

# Only the staging Terraform state owns the group resources; see the file
# header for why.
resource "google_cloud_identity_group" "state_data_access_group" {
  for_each = local.is_production ? toset([]) : local.tf_managed_data_access_group_states

  display_name         = split("@", local.tf_managed_state_data_access_group_emails[each.key])[0]
  description          = "BQ data access group for ${each.key}. Created by Terraform (staging state)."
  parent               = local.workspace_customer_id
  initial_group_config = "EMPTY"

  group_key {
    id = local.tf_managed_state_data_access_group_emails[each.key]
  }

  labels = {
    "cloudidentity.googleapis.com/groups.discussion_forum" = ""
  }

  lifecycle {
    # A group delete drops its human members and breaks the BQ row-level
    # access grants that reference its email. This flag also rejects any plan
    # that removes a state's entry from the YAML while its group instance is
    # still in Terraform state. `removed` blocks cannot target for_each
    # instance keys, so to retire a group:
    # 1. Drop the instance from the staging state:
    #    terraform state rm 'google_cloud_identity_group.state_data_access_group["US_XX"]'
    # 2. Remove the state's entry from state_data_access_groups.yaml.
    # 3. Delete the group by hand in the Workspace admin console.
    prevent_destroy = true
  }
}

# Prod does not own the group resources; it resolves them by email. This
# lookup fails if the group does not exist yet. That only happens when a prod
# plan runs before the staging deploy of the change that added the state.
# Rerun the prod plan after the staging deploy.
data "google_cloud_identity_group_lookup" "state_data_access_group" {
  for_each = local.is_production ? local.tf_managed_data_access_group_states : toset([])

  group_key {
    id = local.tf_managed_state_data_access_group_emails[each.key]
  }
}

# Mirror of ci_cd_sa_data_access_group_manager (state-direct-ingest.tf) for
# TF-managed groups. This is a separate resource because that resource keys
# its for_each by group resource name, which for these groups is only known
# at apply time. State codes are known at plan time.
resource "google_cloud_identity_group_membership" "ci_cd_sa_tf_managed_group_manager" {
  for_each = local.tf_managed_data_access_group_states

  group = local.tf_managed_state_data_access_group_resource_names[each.key]

  preferred_member_key {
    id = "cloud-build-ci-cd@${var.project_id}.iam.gserviceaccount.com"
  }

  roles {
    name = "MEMBER"
  }

  roles {
    name = "MANAGER"
  }
}
