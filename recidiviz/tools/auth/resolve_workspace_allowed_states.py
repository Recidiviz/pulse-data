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
"""Resolves a Recidiviz user's allowedStates straight from Google Workspace,
with no downloaded service-account key.

This is the reference client for the workspace-group-reader service account
(see recidiviz/tools/deploy/terraform/workspace-group-reader.tf). It exists so
that the Polaris auth layer can replace the GCS membership snapshots -- which
drift -- with a live read, and so that we can verify the keyless token flow
end to end before the dashboards team ports it.

Token flow, entirely key-free:
1. Application Default Credentials identify the caller.
2. The IAM Credentials API signs a JWT as workspace-group-reader@{project}
   (requires roles/iam.serviceAccountTokenCreator on that service account).
3. The JWT carries a `sub` claim for a Workspace user, so the resulting token
   acts under domain-wide delegation with the read-only Directory scope.
4. The Directory API lists the user's direct group memberships.

Matches the semantics of the Apps Script sync it replaces: DIRECT memberships
only. A group nested inside a state group does not grant its members that
state (today that affects only s-oz-data, which nests the full-time staff
group).

Usage:
    uv run python -m recidiviz.tools.auth.resolve_workspace_allowed_states \
        --project-id recidiviz-staging \
        --user-email someone@recidiviz.org \
        --delegated-subject <workspace user the reader impersonates>

Prints {"allowedStates": [...]} -- the same shape as the GCS files the Apps
Script sync writes today.
"""
import argparse
import json
import os

import google.auth
import google.auth.iam
import google.auth.transport.requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

import recidiviz.big_query.config as _bq_config_pkg
from recidiviz.big_query.row_access_policy_query_builder import (
    access_group_configs_are_loaded,
)
from recidiviz.utils.yaml_dict import YAMLDict

_DIRECTORY_READONLY_SCOPE = (
    "https://www.googleapis.com/auth/admin.directory.group.readonly"
)
_TOKEN_URI = "https://oauth2.googleapis.com/token"  # nosec B105 -- OAuth endpoint, not a password

_STATE_GROUPS_YAML_PATH = os.path.join(
    os.path.dirname(_bq_config_pkg.__file__), "state_data_access_groups.yaml"
)
_GROUP_SETTINGS_YAML_PATH = os.path.join(
    os.path.dirname(_bq_config_pkg.__file__), "state_data_access_group_settings.yaml"
)


def _load_state_code_by_group_email() -> dict[str, str]:
    """Returns the group email to state code map, derived from the state list
    and the email format in the (mirror-excluded) group config YAMLs."""
    state_codes = YAMLDict.from_path(_STATE_GROUPS_YAML_PATH).raw_yaml.keys()
    settings = YAMLDict.from_path(_GROUP_SETTINGS_YAML_PATH)
    email_format = settings.pop("state_data_group_email_format", str)
    return {
        email_format.replace(
            "{state}", state_code.removeprefix("US_").lower()
        ): state_code
        for state_code in state_codes
    }


def _build_delegated_credentials(
    *, reader_service_account: str, delegated_subject: str
) -> service_account.Credentials:
    """Returns Directory-scoped credentials for the reader service account,
    acting as the delegated Workspace user, minted via the IAM Credentials
    API with no downloaded key."""
    source_credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    signer = google.auth.iam.Signer(
        google.auth.transport.requests.Request(),
        source_credentials,
        reader_service_account,
    )
    return service_account.Credentials(
        signer=signer,
        service_account_email=reader_service_account,
        token_uri=_TOKEN_URI,
        scopes=[_DIRECTORY_READONLY_SCOPE],
        subject=delegated_subject,
    )


def resolve_allowed_states(
    *, user_email: str, reader_service_account: str, delegated_subject: str
) -> list[str]:
    """Returns the state codes whose data access groups hold the user as a
    direct member, sorted, in the same shape the GCS sync files use."""
    if not access_group_configs_are_loaded():
        raise ValueError(
            "The BQ access group configs are not loaded. Run this tool from "
            "the private pulse-data repo with the group config files "
            "present."
        )

    credentials = _build_delegated_credentials(
        reader_service_account=reader_service_account,
        delegated_subject=delegated_subject,
    )
    directory = build("admin", "directory_v1", credentials=credentials)

    group_emails: set[str] = set()
    page_token: str | None = None
    while True:
        response = (
            directory.groups()  # pylint: disable=no-member
            .list(userKey=user_email, pageToken=page_token, maxResults=200)
            .execute()
        )
        for group in response.get("groups", []):
            group_emails.add(group["email"])
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    state_code_by_group_email = _load_state_code_by_group_email()
    return sorted(
        state_code
        for group_email, state_code in state_code_by_group_email.items()
        if group_email in group_emails
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True, type=str)
    parser.add_argument("--user-email", required=True, type=str)
    parser.add_argument(
        "--delegated-subject",
        required=True,
        type=str,
        help="Workspace user the reader impersonates under domain-wide "
        "delegation. Must be allowed to read group memberships.",
    )
    parser.add_argument(
        "--reader-service-account",
        type=str,
        default="",
        help="Defaults to workspace-group-reader@{project-id}.iam.gserviceaccount.com",
    )
    args = parser.parse_args()

    reader = (
        args.reader_service_account
        or f"workspace-group-reader@{args.project_id}.iam.gserviceaccount.com"
    )
    allowed_states = resolve_allowed_states(
        user_email=args.user_email,
        reader_service_account=reader,
        delegated_subject=args.delegated_subject,
    )
    print(json.dumps({"allowedStates": allowed_states}))


if __name__ == "__main__":
    main()
