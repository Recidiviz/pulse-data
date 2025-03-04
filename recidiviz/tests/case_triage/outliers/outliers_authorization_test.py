# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements tests for Outliers authorization."""
import os
from typing import Any, Dict, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock

from flask import Flask, Response, make_response

from recidiviz.case_triage.outliers.outliers_authorization import (
    on_successful_authorization,
)
from recidiviz.utils.auth.auth0 import AuthorizationError, FlaskException

test_app = Flask("test_outliers_authorization")


@test_app.get("/")
def without_state_code() -> Response:
    return make_response("There's no state code here!")


@test_app.get("/<state>/test")
def index(state: str) -> Response:
    return make_response(state)


@mock.patch.dict(os.environ, {"AUTH0_CLAIM_NAMESPACE": "https://recidiviz-test"})
class OutliersAuthorizationClaimsTestCase(TestCase):
    """Tests for Outliers authorization claims processing"""

    def setUp(self) -> None:
        self.endpoint = "test"

    @classmethod
    def _process_claims(
        cls, path: str, claims: Dict[str, Any], offline_mode: bool
    ) -> None:
        with test_app.test_request_context(path=path):
            return on_successful_authorization(claims, offline_mode=offline_mode)

    @classmethod
    def process_claims(
        cls,
        path: str,
        user_state_code: str,
        allowed_states: Optional[list[str]] = None,
        outliers_route_enabled: Optional[bool] = True,
        role: Optional[str] = "leadership_role",
    ) -> None:
        if allowed_states is None:
            allowed_states = []
        return cls._process_claims(
            path,
            {
                "https://recidiviz-test/app_metadata": {
                    "stateCode": user_state_code,
                    "allowedStates": allowed_states,
                    "externalId": "A1B2",
                    "routes": {
                        "insights": outliers_route_enabled,
                    },
                    "role": role,
                },
                "https://recidiviz-test/email_address": "test@recidiviz.org",
            },
            offline_mode=False,
        )

    @classmethod
    def process_offline_claim(cls, path: str) -> None:
        return cls._process_claims(
            path,
            {
                "https://recidiviz-test/email_address": "offline@recidiviz.org",
            },
            offline_mode=True,
        )

    @mock.patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
        return_value=["US_IX"],
    )
    def test_recidiviz_auth(self, _mock_enabled_states: MagicMock) -> None:
        # Recidiviz users can access a outliers enabled state if they have the state in allowed_states
        self.assertIsNone(
            self.process_claims(
                f"/US_IX/{self.endpoint}",
                user_state_code="recidiviz",
                allowed_states=["US_IX"],
            )
        )

        # Recidiviz users cannot access a state that does not have outliers enabled
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(
                f"/US_WY/{self.endpoint}",
                user_state_code="recidiviz",
                allowed_states=["US_WY"],
            )
            self.assertEqual(assertion.exception.code, "state_not_enabled")

        # Recidiviz users cannot access a Outliers enabled state that is not in their list of allowed_states
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(
                f"/US_IX/{self.endpoint}",
                user_state_code="recidiviz",
                allowed_states=["US_CA"],
            )
            self.assertEqual(assertion.exception.code, "recidiviz_user_not_authorized")

    @mock.patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
        return_value=["US_IX", "US_PA"],
    )
    def test_on_successful_authorization(self, _mock_enabled_states: MagicMock) -> None:
        # State users can access their own Outliers enabled state
        self.assertIsNone(
            self.process_claims(f"/US_IX/{self.endpoint}", user_state_code="US_IX")
        )

        # Other state users who have outliers enabled cannot access other states
        with self.assertRaises(AuthorizationError):
            self.process_claims(f"/US_IX/{self.endpoint}", user_state_code="US_PA")

        # Other state users who do not have outliers enabled cannot access other states
        with self.assertRaises(AuthorizationError):
            self.process_claims(f"/US_IX/{self.endpoint}", user_state_code="US_WY")

        # Other users cannot access a state that does not have outliers enabled
        with self.assertRaises(FlaskException):
            self.process_claims(f"/US_WY/{self.endpoint}", user_state_code="US_IX")

        # State users cannot access their state if it does not have outliers enabled
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(f"/US_WY/{self.endpoint}", user_state_code="US_WY")
            self.assertEqual(assertion.exception.code, "state_not_enabled")

        # State users cannot access endpoints that are set to false in their routes
        with self.assertRaises(FlaskException):
            self.process_claims(
                f"/US_IX/{self.endpoint}",
                user_state_code="US_IX",
                outliers_route_enabled=False,
            )

        # State user can access endpoints that are set to true in their routes
        self.assertIsNone(
            self.process_claims(
                f"/US_IX/{self.endpoint}",
                user_state_code="US_IX",
            )
        )

    @mock.patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
        return_value=["US_MI", "US_PA"],
    )
    def test_csg_auth(self, _mock_enabled_states: MagicMock) -> None:
        # Allowed state, allowed endpoint
        self.assertIsNone(self.process_claims("/US_MI/test", user_state_code="CSG"))

        # Enabled state, but not CSG state
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(f"/US_PA/{self.endpoint}", user_state_code="CSG")

        self.assertEqual(assertion.exception.code, "csg_user_not_authorized")

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

    def test_offline_mode(self) -> None:
        self.assertIsNone(self.process_offline_claim(f"/US_OZ/{self.endpoint}"))

        with self.assertRaises(FlaskException) as assertion:
            self.process_offline_claim(f"/US_CA/{self.endpoint}")
        self.assertEqual(assertion.exception.code, "offline_state_required")
