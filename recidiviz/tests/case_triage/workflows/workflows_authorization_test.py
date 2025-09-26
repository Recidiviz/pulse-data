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
from typing import Any, Dict, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock

from flask import Flask, Response, make_response

from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)
from recidiviz.utils.auth.auth0 import AuthorizationError, FlaskException

test_app = Flask("test_workflows_authorization")


@test_app.get("/external_request/<state>/enqueue_sms_request")
def index(state: str) -> Response:
    return make_response(state)


@test_app.get("/")
def without_state_code() -> Response:
    return make_response("There's no state code here!")


@mock.patch.dict(os.environ, {"AUTH0_CLAIM_NAMESPACE": "https://recidiviz-test"})
class WorkflowsAuthorizationClaimsTestCase(TestCase):
    """Tests for workflows authorization claims processing"""

    @classmethod
    def _process_claims(cls, path: str, claims: Dict[str, Any]) -> None:
        with test_app.test_request_context(path=path):
            return on_successful_authorization(claims)

    @classmethod
    def process_claims(
        cls,
        path: str,
        user_state_code: str,
        allowed_states: Optional[list[str]] = None,
        feature_variants: Optional[dict[str, Any]] = None,
    ) -> None:
        if allowed_states is None:
            allowed_states = []
        if feature_variants is None:
            feature_variants = {}
        return cls._process_claims(
            path,
            {
                "https://recidiviz-test/app_metadata": {
                    "stateCode": user_state_code,
                    "allowedStates": allowed_states,
                    "featureVariants": feature_variants,
                }
            },
        )

    @mock.patch(
        "recidiviz.case_triage.workflows.workflows_authorization.get_workflows_enabled_states",
        return_value=["US_CA", "US_TN"],
    )
    def test_recidiviz_auth(self, _mock_enabled_states: MagicMock) -> None:
        # Recidiviz users can make external requests for a workflows state if they have the state in allowed_states
        self.assertIsNone(
            self.process_claims(
                "external_request/US_CA/enqueue_sms_request",
                user_state_code="recidiviz",
                allowed_states=["US_CA"],
            )
        )

        # Recidiviz users cannot send external requests to a workflows enabled state that is not in their list of allowed_states
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(
                "external_request/US_TN/enqueue_sms_request",
                user_state_code="recidiviz",
                allowed_states=["US_CA"],
            )
            self.assertEqual(assertion.exception.code, "recidiviz_user_not_authorized")

    @mock.patch(
        "recidiviz.case_triage.workflows.workflows_authorization.get_workflows_enabled_states",
        return_value=["US_CA", "US_TN"],
    )
    def test_on_successful_authorization(self, _mock_enabled_states: MagicMock) -> None:
        # State users can send external requests for their own workflows enabled state
        self.assertIsNone(
            self.process_claims(
                "external_request/US_TN/enqueue_sms_request", user_state_code="US_TN"
            )
        )

        self.assertIsNone(
            self.process_claims(
                "external_request/US_CA/enqueue_sms_request", user_state_code="US_CA"
            )
        )

        # Other state users can not send external requests to other states
        with self.assertRaises(AuthorizationError):
            self.process_claims(
                "external_request/US_CA/enqueue_sms_request", user_state_code="US_TN"
            )

        # Other state users who do not have workflows external requests enabled cannot access other states
        with self.assertRaises(AuthorizationError):
            self.process_claims(
                "external_request/US_CA/enqueue_sms_request", user_state_code="US_WY"
            )

        # Other users cannot access a state that does not have workflows external requests enabled
        with self.assertRaises(FlaskException):
            self.process_claims(
                "external_request/US_WY/enqueue_sms_request", user_state_code="US_CA"
            )

        # State users cannot access their state if it does not have pathways enabled
        with self.assertRaises(FlaskException) as assertion:
            self.process_claims(
                "external_request/US_WY/enqueue_sms_request", user_state_code="US_WY"
            )
            self.assertEqual(assertion.exception.code, "external_requests_not_enabled")
