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

# Read-only identity for resolving state data access group membership straight
# from Google Workspace. This replaces the Apps Script that copied membership
# snapshots into GCS for the Polaris apps to read: the snapshots drifted from
# reality three times during the 2026-09 US_NYC launch, so the apps move to
# reading the source directly.
#
# The service account holds NO GCP roles. Its only power is Workspace
# domain-wide delegation (DWD), scoped to the read-only Directory API scope
# https://www.googleapis.com/auth/admin.directory.group.readonly, which a
# Workspace super admin registers by hand at admin.google.com > Security >
# API controls > Domain-wide delegation, using the OAuth client ID from the
# output below.
#
# Consumers, by approved credential path:
# - The Auth0 staff-tenant login action (update-user-restrictions.js in
#   pulse-dashboards) holds a hand-created key for this account in its action
#   secrets. The security team approved vendor-held secrets for Auth0; the
#   account's blast radius stays "read group memberships, nothing else".
#   Do not manage that key in Terraform -- a Terraform-managed key would sit
#   in the state bucket.
# - GCP-side callers and local verification mint short-lived tokens through
#   the IAM Credentials API (signJwt) instead; grant
#   roles/iam.serviceAccountTokenCreator per caller. Reference client:
#   recidiviz/tools/auth/resolve_workspace_allowed_states.py.

resource "google_project_service" "iam_credentials_api" {
  service = "iamcredentials.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

# Directory API calls made with this service account's credentials ride on
# this project's API enablement, so the Admin SDK API must be on here even
# though the data it reads lives in Workspace, not in this project.
resource "google_project_service" "admin_sdk_api" {
  service = "admin.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

resource "google_service_account" "workspace_group_reader" {
  account_id   = "workspace-group-reader"
  display_name = "Workspace group reader"
  description  = "Read-only Directory API access via domain-wide delegation, used to resolve state data access group membership. Holds no GCP roles."
}

output "workspace_group_reader_oauth_client_id" {
  description = "OAuth client ID to register for domain-wide delegation, with scope admin.directory.group.readonly."
  value       = google_service_account.workspace_group_reader.unique_id
}
