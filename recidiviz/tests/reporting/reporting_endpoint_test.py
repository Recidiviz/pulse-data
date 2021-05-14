# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for reporting/email_generation.py."""
import json
from http import HTTPStatus
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import patch, MagicMock

import flask
from flask import Flask

from recidiviz.common.results import MultiRequestResult
from recidiviz.reporting.reporting_endpoint import reporting_endpoint_blueprint

FIXTURE_FILE = "po_monthly_report_data_fixture.json"


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
class ReportingEndpointTests(TestCase):
    """ Integration tests of our flask endpoints """

    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.register_blueprint(reporting_endpoint_blueprint)
        self.app.config["TESTING"] = True

        self.client = self.app.test_client()
        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}

        with self.app.test_request_context():
            self.start_new_batch_url = flask.url_for(
                "reporting_endpoint_blueprint.start_new_batch"
            )
            self.deliver_emails_for_batch_url = flask.url_for(
                "reporting_endpoint_blueprint.deliver_emails_for_batch"
            )
            self.state_code = "US_ID"

    def test_start_new_batch_validation(self) -> None:
        with self.app.test_request_context():
            base_query_string = {
                "state_code": self.state_code,
                "report_type": "po_monthly_report",
            }
            response = self.client.get(self.start_new_batch_url, headers=self.headers)

            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                response.data,
                b"Request does not include 'state_code' and/or 'report_type' parameters",
            )

            # Invalid test address
            response = self.client.get(
                self.start_new_batch_url,
                query_string={**base_query_string, "test_address": "fake-email"},
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Invalid email address format: [fake-email]", response.data
            )

            # Invalid recipient email
            response = self.client.get(
                self.start_new_batch_url,
                query_string={
                    **base_query_string,
                    "email_allowlist": json.dumps(["dev@recidiviz.org", "foo"]),
                },
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(b"Invalid email address format: [foo]", response.data)

    @patch("recidiviz.reporting.email_reporting_utils.generate_batch_id")
    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_start_new_batch(
        self, mock_start: MagicMock, mock_generate: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mock_generate.return_value = "test_batch_id"
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )

            response = self.client.get(
                self.start_new_batch_url,
                query_string={
                    "state_code": self.state_code,
                    "report_type": "po_monthly_report",
                    "email_allowlist": json.dumps(
                        ["dev@recidiviz.org", "other@recidiviz.org"]
                    ),
                },
                headers=self.headers,
            )

            mock_start.assert_called_with(
                state_code=self.state_code,
                report_type="po_monthly_report",
                batch_id="test_batch_id",
                test_address=None,
                region_code=None,
                email_allowlist=["dev@recidiviz.org", "other@recidiviz.org"],
                message_body=None,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertIn(
                f"New batch started for {self.state_code}", str(response.data)
            )

    @patch("recidiviz.reporting.email_reporting_utils.generate_batch_id")
    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_start_new_batch_with_no_allowlist(
        self, mock_start: MagicMock, mock_generate: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mock_generate.return_value = "test_batch_id"
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["letter@kenny.ca"], failures=[]
            )

            response = self.client.get(
                self.start_new_batch_url,
                query_string={
                    "state_code": self.state_code,
                    "report_type": "po_monthly_report",
                },
                headers=self.headers,
            )

            mock_start.assert_called_with(
                state_code=self.state_code,
                report_type="po_monthly_report",
                batch_id="test_batch_id",
                test_address=None,
                region_code=None,
                email_allowlist=None,
                message_body=None,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertIn(
                f"New batch started for {self.state_code}", str(response.data)
            )

    @patch("recidiviz.reporting.email_reporting_utils.generate_batch_id")
    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_counts_messages(
        self, mock_start: MagicMock, mock_generate: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mock_generate.return_value = "test_batch_id"
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )
            response = self.client.get(
                self.start_new_batch_url,
                query_string={
                    "state_code": self.state_code,
                    "report_type": "po_monthly_report",
                    "email_allowlist": json.dumps(
                        ["dev@recidiviz.org", "other@recidiviz.org"]
                    ),
                },
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertIn(
                f"New batch started for {self.state_code}", str(response.data)
            )
            self.assertIn("Successfully generated 1 email(s)", str(response.data))
            self.assertNotIn("Failed to generate", str(response.data))

            mock_start.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"],
                failures=["other@recidiviz.org", "letter@kenny.ca"],
            )

            response = self.client.get(
                self.start_new_batch_url,
                query_string={
                    "state_code": self.state_code,
                    "report_type": "po_monthly_report",
                    "email_allowlist": json.dumps(
                        ["dev@recidiviz.org", "other@recidiviz.org"]
                    ),
                },
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.MULTI_STATUS, response.status_code)
            self.assertIn(
                f"New batch started for {self.state_code}", str(response.data)
            )
            self.assertIn("Successfully generated 1 email(s)", str(response.data))
            self.assertIn("Failed to generate 2 email(s)", str(response.data))

            mock_start.return_value = MultiRequestResult[str, str](
                successes=[],
                failures=[
                    "dev@recidiviz.org",
                    "other@recidiviz.org",
                    "letter@kenny.ca",
                ],
            )
            response = self.client.get(
                self.start_new_batch_url,
                query_string={
                    "state_code": self.state_code,
                    "report_type": "po_monthly_report",
                    "email_allowlist": json.dumps(
                        ["dev@recidiviz.org", "other@recidiviz.org"]
                    ),
                },
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)

    @patch("recidiviz.reporting.email_delivery.deliver")
    def test_integration_deliver_emails_for_batch(
        self, mock_deliver: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )
            response = self.client.get(
                self.deliver_emails_for_batch_url,
                query_string={
                    "batch_id": "test_batch_id",
                    "state_code": self.state_code,
                },
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=["other@recidiviz.org"]
            )
            response = self.client.get(
                self.deliver_emails_for_batch_url,
                query_string={
                    "batch_id": "test_batch_id",
                    "state_code": self.state_code,
                },
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.MULTI_STATUS, response.status_code)

            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=[], failures=["dev@recidiviz.org", "other@recidiviz.org"]
            )
            response = self.client.get(
                self.deliver_emails_for_batch_url,
                query_string={
                    "batch_id": "test_batch_id",
                    "state_code": self.state_code,
                },
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
