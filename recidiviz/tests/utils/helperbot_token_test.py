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
"""Tests that validate Helperbot GitHub token permissions.

These tests only run in GitHub Actions (where HELPERBOT_TOKEN /
GH_HELPERBOT_TOKEN are available as environment variables). They are skipped
when run locally.

To run manually in CI, trigger the test-helperbot-tokens workflow or pass the
token via environment variable:

    HELPERBOT_TOKEN=ghp_... pytest recidiviz/tests/utils/helperbot_token_test.py
"""
import os
import unittest

import requests

# The scopes that HELPERBOT_TOKEN must have for all workflows that use it:
#   - repo: PR operations, private repo access, push (tbr-auto-approval,
#     automerge-dependabot, sync-looker-repo, copybara, pii-check,
#     rebuild-runner-amis, deploy_to_jc_playtest)
#   - workflow: workflow operations (copybara)
#   - read:org: resolve org team slugs like @Recidiviz/infra-review
#     (tbr-auto-approval)
HELPERBOT_TOKEN_REQUIRED_SCOPES = {"repo", "workflow", "read:org"}

# GH_HELPERBOT_TOKEN is used by fewer workflows and does not need read:org:
#   - repo: issue access, private repo access (find-closed-todos,
#     jc_continuous_staging_deploy)
#   - workflow: workflow operations
GH_HELPERBOT_TOKEN_REQUIRED_SCOPES = {"repo", "workflow"}

GITHUB_API_BASE = "https://api.github.com"


def _get_token_scopes(token: str) -> set[str]:
    """Returns the set of OAuth scopes granted to a classic GitHub PAT.

    Raises AssertionError if the token appears to be a fine-grained PAT
    (which does not return OAuth scopes) or if the API call fails.
    """
    response = requests.head(
        f"{GITHUB_API_BASE}/user",
        headers={"Authorization": f"bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()

    raw_scopes = response.headers.get("X-OAuth-Scopes", "")
    if not raw_scopes:
        raise AssertionError(
            "No OAuth scopes returned. The token may be a fine-grained PAT "
            "(expected a classic PAT) or may be invalid."
        )

    return {s.strip() for s in raw_scopes.split(",")}


def _check_api_access(token: str, endpoint: str) -> int:
    """Makes a GET request to a GitHub API endpoint and returns the status code."""
    response = requests.get(
        f"{GITHUB_API_BASE}{endpoint}",
        headers={"Authorization": f"bearer {token}"},
        timeout=10,
    )
    return response.status_code


@unittest.skipUnless(
    os.environ.get("HELPERBOT_TOKEN"),
    "HELPERBOT_TOKEN not set (only runs in GitHub Actions)",
)
class TestHelperbotTokenPermissions(unittest.TestCase):
    """Validates that HELPERBOT_TOKEN has the scopes and access needed by CI
    workflows that depend on it."""

    def setUp(self) -> None:
        token = os.environ.get("HELPERBOT_TOKEN")
        assert token is not None
        self.token: str = token

    def test_is_classic_pat(self) -> None:
        """HELPERBOT_TOKEN must be a classic PAT (prefix ghp_), not a
        fine-grained token. Fine-grained tokens do not support OAuth scopes
        and have caused CI failures in the past."""
        self.assertTrue(
            self.token.startswith("ghp_"),
            "HELPERBOT_TOKEN does not start with 'ghp_'. "
            "Expected a classic PAT, got what appears to be a fine-grained token.",
        )

    def test_has_required_scopes(self) -> None:
        scopes = _get_token_scopes(self.token)
        missing = HELPERBOT_TOKEN_REQUIRED_SCOPES - scopes
        self.assertFalse(
            missing,
            f"HELPERBOT_TOKEN is missing required scopes: {missing}. "
            f"Granted scopes: {scopes}. "
            f"Affected workflows: tbr-auto-approval, automerge-dependabot, "
            f"sync-looker-repo, copybara, pii-check, rebuild-runner-amis, "
            f"deploy_to_jc_playtest.",
        )

    def test_can_resolve_org_teams(self) -> None:
        """The tbr-auto-approval workflow needs to resolve @Recidiviz/infra-review."""
        status = _check_api_access(self.token, "/orgs/Recidiviz/teams/infra-review")
        self.assertEqual(
            status,
            200,
            f"Cannot resolve @Recidiviz/infra-review (HTTP {status}). "
            f"The read:org scope may be missing.",
        )

    def test_can_access_recidiviz_data(self) -> None:
        status = _check_api_access(self.token, "/repos/Recidiviz/pulse-data")
        self.assertEqual(
            status,
            200,
            f"Cannot access Recidiviz/pulse-data (HTTP {status}).",
        )

    def test_can_access_looker_repo(self) -> None:
        """The sync-looker-repo workflow needs cross-repo access."""
        status = _check_api_access(self.token, "/repos/Recidiviz/looker")
        self.assertEqual(
            status,
            200,
            f"Cannot access Recidiviz/looker (HTTP {status}). "
            f"Cross-repo sync will fail.",
        )


@unittest.skipUnless(
    os.environ.get("GH_HELPERBOT_TOKEN"),
    "GH_HELPERBOT_TOKEN not set (only runs in GitHub Actions)",
)
class TestGhHelperbotTokenPermissions(unittest.TestCase):
    """Validates that GH_HELPERBOT_TOKEN has the scopes and access needed by CI
    workflows that depend on it."""

    def setUp(self) -> None:
        token = os.environ.get("GH_HELPERBOT_TOKEN")
        assert token is not None
        self.token: str = token

    def test_is_classic_pat(self) -> None:
        self.assertTrue(
            self.token.startswith("ghp_"),
            "GH_HELPERBOT_TOKEN does not start with 'ghp_'. "
            "Expected a classic PAT, got what appears to be a fine-grained token.",
        )

    def test_has_required_scopes(self) -> None:
        scopes = _get_token_scopes(self.token)
        missing = GH_HELPERBOT_TOKEN_REQUIRED_SCOPES - scopes
        self.assertFalse(
            missing,
            f"GH_HELPERBOT_TOKEN is missing required scopes: {missing}. "
            f"Granted scopes: {scopes}. "
            f"Affected workflows: find-closed-todos, jc_continuous_staging_deploy.",
        )

    def test_can_access_recidiviz_data(self) -> None:
        status = _check_api_access(self.token, "/repos/Recidiviz/pulse-data")
        self.assertEqual(
            status,
            200,
            f"Cannot access Recidiviz/pulse-data (HTTP {status}).",
        )
