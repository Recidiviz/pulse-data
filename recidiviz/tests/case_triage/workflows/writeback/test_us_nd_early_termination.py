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
"""Tests for the US_ND early termination writeback."""
from datetime import date
from typing import Generator
from unittest import TestCase

import pytest
import requests
import responses
from flask import Flask
from mock import MagicMock, patch
from pydantic import ValidationError
from responses import matchers

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.us_nd_early_termination import (
    JustificationReason,
    UsNdEarlyTerminationRequestData,
    UsNdEarlyTerminationStatusTracker,
    UsNdEarlyTerminationWritebackExecutor,
)

EARLY_TERMINATION_PEI = 123
USER_EMAIL = "foo@nd.gov"
EARLY_TERMINATION_DATE = date(2024, 10, 10)
JUSTIFICATION_REASONS = [
    JustificationReason(code="FOO", description="Code FOO"),
    JustificationReason(code="BAR", description="Code BAR"),
]

MODULE = "recidiviz.case_triage.workflows.writeback.us_nd_early_termination"
TRANSPORT_MODULE = "recidiviz.case_triage.workflows.writeback.transports.rest"


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_us_nd_writeback")
    with test_app.app_context():
        yield


class TestUsNdEarlyTerminationWritebackExecutor(TestCase):
    """Tests for UsNdEarlyTerminationWritebackExecutor."""

    def setUp(self) -> None:
        self.fake_url = "http://fake-url.com"

    def test_to_cloud_task_payload(self) -> None:
        request_data = UsNdEarlyTerminationRequestData(
            person_external_id=EARLY_TERMINATION_PEI,
            user_email=USER_EMAIL,
            early_termination_date=EARLY_TERMINATION_DATE,
            justification_reasons=JUSTIFICATION_REASONS,
            should_queue_task=True,
        )
        executor = UsNdEarlyTerminationWritebackExecutor(request_data)

        self.assertEqual(
            executor.to_cloud_task_payload(),
            {
                "should_queue_task": False,
                "person_external_id": EARLY_TERMINATION_PEI,
                "user_email": USER_EMAIL,
                "early_termination_date": EARLY_TERMINATION_DATE.isoformat(),
                "justification_reasons": [
                    {"code": "FOO", "description": "Code FOO"},
                    {"code": "BAR", "description": "Code BAR"},
                ],
            },
        )

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_success(
        self, _mock_client: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(
                responses.PUT,
                self.fake_url,
                json=response_json,
                match=[
                    matchers.json_params_matcher(
                        {
                            "sid": EARLY_TERMINATION_PEI,
                            "userEmail": USER_EMAIL,
                            "earlyTerminationDate": EARLY_TERMINATION_DATE.isoformat(),
                            "justificationReasons": [
                                {"code": "FOO", "description": "Code FOO"},
                                {"code": "BAR", "description": "Code BAR"},
                            ],
                        }
                    )
                ],
            )
            request_data = UsNdEarlyTerminationRequestData(
                person_external_id=EARLY_TERMINATION_PEI,
                user_email=USER_EMAIL,
                early_termination_date=EARLY_TERMINATION_DATE,
                justification_reasons=JUSTIFICATION_REASONS,
            )
            writeback = UsNdEarlyTerminationWritebackExecutor(request_data)
            writeback.execute()

    @patch("requests.put")
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_missing_secrets(
        self, _mock_client: MagicMock, mock_get_secret: MagicMock, mock_put: MagicMock
    ) -> None:
        mock_get_secret.return_value = None
        with self.assertRaises(EnvironmentError):
            request_data = UsNdEarlyTerminationRequestData(
                person_external_id=EARLY_TERMINATION_PEI,
                user_email=USER_EMAIL,
                early_termination_date=EARLY_TERMINATION_DATE,
                justification_reasons=JUSTIFICATION_REASONS,
            )
            writeback = UsNdEarlyTerminationWritebackExecutor(request_data)
            writeback.execute()
        mock_put.assert_not_called()

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    @patch(f"{MODULE}.in_gcp_production")
    def test_execute_prod_recidiviz_user(
        self,
        mock_in_prod: MagicMock,
        _mock_client: MagicMock,
        mock_get_secret: MagicMock,
    ) -> None:
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        mock_in_prod.return_value = True
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json=response_json)
            request_data = UsNdEarlyTerminationRequestData(
                person_external_id=EARLY_TERMINATION_PEI,
                user_email="internal@recidiviz.org",
                early_termination_date=EARLY_TERMINATION_DATE,
                justification_reasons=JUSTIFICATION_REASONS,
            )
            writeback = UsNdEarlyTerminationWritebackExecutor(request_data)
            writeback.execute()

            # When use_test_url=True, transport fetches test URL secret first
            mock_get_secret.assert_any_call(
                "workflows_us_nd_early_termination_test_url"
            )

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_network_error(
        self, _mock_client: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, body=ConnectionRefusedError())
            with self.assertRaises(ConnectionRefusedError):
                request_data = UsNdEarlyTerminationRequestData(
                    person_external_id=EARLY_TERMINATION_PEI,
                    user_email="internal@recidiviz.org",
                    early_termination_date=EARLY_TERMINATION_DATE,
                    justification_reasons=JUSTIFICATION_REASONS,
                )
                writeback = UsNdEarlyTerminationWritebackExecutor(request_data)
                writeback.execute()

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_http_error(
        self, _mock_client: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, status=500)
            with self.assertRaises(requests.exceptions.HTTPError):
                request_data = UsNdEarlyTerminationRequestData(
                    person_external_id=EARLY_TERMINATION_PEI,
                    user_email="internal@recidiviz.org",
                    early_termination_date=EARLY_TERMINATION_DATE,
                    justification_reasons=JUSTIFICATION_REASONS,
                )
                writeback = UsNdEarlyTerminationWritebackExecutor(request_data)
                writeback.execute()


class TestUsNdEarlyTerminationRequestData(TestCase):
    """Tests model validation for UsNdEarlyTerminationRequestData"""

    def test_valid_camel_case(self) -> None:
        data = UsNdEarlyTerminationRequestData.model_validate(
            {
                "personExternalId": 1234,
                "userEmail": USER_EMAIL,
                "earlyTerminationDate": "2024-10-10",
                "justificationReasons": [
                    {"code": "FOO", "description": "Code FOO"},
                ],
            }
        )
        self.assertEqual(data.person_external_id, 1234)
        self.assertEqual(data.early_termination_date, date(2024, 10, 10))

    def test_valid_snake_case(self) -> None:
        data = UsNdEarlyTerminationRequestData.model_validate(
            {
                "person_external_id": 1234,
                "user_email": USER_EMAIL,
                "early_termination_date": "2024-10-10",
                "justification_reasons": [
                    {"code": "FOO", "description": "Code FOO"},
                ],
            }
        )
        self.assertEqual(data.user_email, USER_EMAIL)

    def test_missing_field(self) -> None:
        with self.assertRaises(ValidationError):
            UsNdEarlyTerminationRequestData.model_validate(
                {
                    "person_external_id": 1234,
                    "user_email": USER_EMAIL,
                    "early_termination_date": "2024-10-10",
                }
            )

    def test_invalid_date(self) -> None:
        with self.assertRaises(ValidationError):
            UsNdEarlyTerminationRequestData.model_validate(
                {
                    "person_external_id": 1234,
                    "user_email": USER_EMAIL,
                    "early_termination_date": "1/1/2024",
                    "justification_reasons": [
                        {"code": "FOO", "description": "Code FOO"},
                    ],
                }
            )

    def test_string_pei_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            UsNdEarlyTerminationRequestData.model_validate(
                {
                    "person_external_id": "A1234",
                    "user_email": USER_EMAIL,
                    "early_termination_date": "2024-10-10",
                    "justification_reasons": [
                        {"code": "FOO", "description": "Code FOO"},
                    ],
                }
            )

    def test_malformed_justification(self) -> None:
        with self.assertRaises(ValidationError):
            UsNdEarlyTerminationRequestData.model_validate(
                {
                    "person_external_id": 1234,
                    "user_email": USER_EMAIL,
                    "early_termination_date": "2024-10-10",
                    "justification_reasons": [{"code": "FOO"}],
                }
            )


class TestUsNdEarlyTerminationStatusTracker(TestCase):
    def test_set_status_updates_firestore(self) -> None:
        mock_firestore = MagicMock()
        mock_firestore.timestamp_key = "serverTimestamp"
        tracker = UsNdEarlyTerminationStatusTracker(123, mock_firestore)
        tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)

        mock_firestore.update_document.assert_called_once()
        call_args = mock_firestore.update_document.call_args
        self.assertEqual(
            call_args[0][0],
            "clientUpdatesV2/us_nd_123/clientOpportunityUpdates/earlyTermination",
        )
        self.assertEqual(
            call_args[0][1]["omsSnooze.status"],
            ExternalSystemRequestStatus.IN_PROGRESS.value,
        )
