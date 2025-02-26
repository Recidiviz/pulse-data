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
"""Implements tests for Pathways authorization."""
import os
from unittest import TestCase, mock
from unittest.mock import MagicMock

from flask import Flask, Response, make_response

from recidiviz.case_triage.pathways.pathways_authorization import (
    on_successful_authorization,
)
from recidiviz.utils.auth.auth0 import AuthorizationError, FlaskException

test_app = Flask("test_pathways_authorization")


@test_app.get("/<state>/<metric_name>")
def index(state: str, metric_name: str) -> Response:
    return make_response(state, metric_name)


@test_app.get("/")
def without_state_code() -> Response:
    return make_response("There's no state code here!")


@mock.patch.dict(os.environ, {"AUTH0_CLAIM_NAMESPACE": "https://recidiviz-test"})
class PathwaysAuthorizationClaimsTestCase(TestCase):
    """Tests for pathways authorization claims processing"""

    def setUp(self) -> None:
        self.endpoint = "PrisonPopulationOverTimeCount"

    @classmethod
    def process_claims(cls, path: str, user_state_code: str) -> None:
        with test_app.test_request_context(path=path):
            return on_successful_authorization(
                {
                    "https://recidiviz-test/app_metadata": {
                        "state_code": user_state_code,
                        "routes": {
                            "system_prison": True,
                            "system_prisonToSupervision": False,
                            "system_supervision": False,
                            "system_supervisionToLiberty": True,
                        },
                    }
                }
            )

    @mock.patch(
        "recidiviz.case_triage.pathways.pathways_authorization.get_pathways_enabled_states",
        return_value=["US_CA", "US_OR"],
    )
    def test_on_successful_authorization(self, _mock_enabled_states: MagicMock) -> None:
        # Recidiviz users can access a pathways enabled state
        self.assertIsNone(
            self.process_claims(f"/US_CA/{self.endpoint}", user_state_code="recidiviz")
        )

        # State users can access their own pathways enabled state
        self.assertIsNone(
            self.process_claims(f"/US_CA/{self.endpoint}", user_state_code="US_CA")
        )

        # Other state users who have pathways enabled cannot access other states
        with self.assertRaises(AuthorizationError):
            self.process_claims(f"/US_CA/{self.endpoint}", user_state_code="US_OR")

        # Other state users who do not have pathways enabled cannot access other states
        with self.assertRaises(AuthorizationError):
            self.process_claims(f"/US_CA/{self.endpoint}", user_state_code="US_WY")

        # Recidiviz users cannot access a state that does not have pathways enabled
        with self.assertRaises(FlaskException):
            self.process_claims(f"/US_WY/{self.endpoint}", user_state_code="recidiviz")

        # Other users cannot access a state that does not have pathways enabled
        with self.assertRaises(FlaskException):
            self.process_claims(f"/US_WY/{self.endpoint}", user_state_code="US_CA")

        # State users cannot access their state if it does not have pathways enabled
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(f"/US_WY/{self.endpoint}", user_state_code="US_WY")
            self.assertEqual(assertion.exception.code, "pathways_not_enabled")

        # State users cannot access endpoints that aren't in their routes
        with self.assertRaises(FlaskException):
            self.process_claims(
                "/US_CA/LibertyToPrisonTransitions", user_state_code="US_CA"
            )

        # State users cannot access endpoints that are set to false in their routes
        with self.assertRaises(FlaskException):
            self.process_claims(
                "/US_CA/PrisonToSupervisionTransitions", user_state_code="US_CA"
            )
        with self.assertRaises(FlaskException):
            self.process_claims(
                "/US_CA/SupervisionPopulationOverTimeCount", user_state_code="US_CA"
            )

        # State user can access endpoints that are set to true in their routes
        self.assertIsNone(
            self.process_claims(
                "/US_CA/PrisonPopulationOverTimeCount", user_state_code="US_CA"
            )
        )
        self.assertIsNone(
            self.process_claims(
                "/US_CA/SupervisionToLibertyTransitions", user_state_code="US_CA"
            )
        )

        # Recidiviz users can access any endpoint
        self.assertIsNone(
            self.process_claims(
                "/US_CA/LibertyToPrisonTransitions", user_state_code="recidiviz"
            )
        )
        self.assertIsNone(
            self.process_claims(
                "/US_CA/PrisonToSupervisionTransitions", user_state_code="recidiviz"
            )
        )
        self.assertIsNone(
            self.process_claims(
                "/US_CA/SupervisionPopulationOverTimeCount", user_state_code="recidiviz"
            )
        )

    def test_invalid_state_code(self) -> None:
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(
                f"/US_FAKE/{self.endpoint}", user_state_code="recidiviz"
            )

        self.assertEqual(assertion.exception.code, "valid_state_required")

        with self.assertRaises(FlaskException):
            self.process_claims(f"/US_FAKE/{self.endpoint}", user_state_code="US_CA")

        # If there is no state_code view_arg, we raise an Authorization error
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims("/", user_state_code="recidiviz")

        self.assertEqual(assertion.exception.code, "state_required")
