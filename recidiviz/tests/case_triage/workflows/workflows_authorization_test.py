# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements tests for Workflows authorization."""
import os
from typing import Any, Dict
from unittest import TestCase, mock

from flask import Flask

from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)
from recidiviz.utils.auth.auth0 import AuthorizationError

test_app = Flask("test_workflows_authorization")


@mock.patch.dict(os.environ, {"AUTH0_CLAIM_NAMESPACE": "https://recidiviz-test"})
class WorkflowsAuthorizationClaimsTestCase(TestCase):
    """Tests for Workflows authorization claims processing"""

    @classmethod
    def _process_claims(cls, path: str, claims: Dict[str, Any]) -> None:
        with test_app.test_request_context(path=path):
            return on_successful_authorization(claims)

    @classmethod
    def process_claims(cls, path: str, user_state_code: str) -> None:
        return cls._process_claims(
            path,
            {
                "https://recidiviz-test/app_metadata": {
                    "state_code": user_state_code,
                }
            },
        )

    @classmethod
    def process_offline_claim(cls, path: str) -> None:
        return cls._process_claims(path, {})

    def test_on_successful_authorization(self) -> None:
        # Recidiviz users can access any endpoint
        self.assertIsNone(
            self.process_claims("/US_TN/init", user_state_code="recidiviz")
        )

        self.assertIsNone(
            self.process_claims(
                "/external_request/US_TN/insert_contact_note",
                user_state_code="recidiviz",
            )
        )

        # Non-recidiviz users cannot access endpoints
        with self.assertRaises(AuthorizationError):
            self.process_claims("/US_TN/init", user_state_code="US_OR")

        with self.assertRaises(AuthorizationError):
            self.process_claims(
                "/external_request/US_TN/insert_contact_note", user_state_code="US_TN"
            )
