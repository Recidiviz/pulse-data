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
"""Tests for admin_panel/routes/line_staff_tools.py"""
import datetime
import json
from http import HTTPStatus
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

import flask
from flask import Blueprint, Flask

from recidiviz.admin_panel.all_routes import admin_stores
from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResult
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

FIXTURE_FILE = "po_monthly_report_data_fixture.json"


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
class ReportingEndpointTests(TestCase):
    """Integration tests of our flask endpoints"""

    def setUp(self) -> None:
        self.gcs_file_system_patcher = patch(
            "recidiviz.cloud_storage.gcsfs_factory.GcsfsFactory.build"
        )
        self.requires_gae_auth_patcher = patch(
            "recidiviz.admin_panel.routes.line_staff_tools.requires_gae_auth",
            side_effect=lambda route: route,
        )

        self.requires_gae_auth_patcher.start()

        self.gcs_file_system = FakeGCSFileSystem()
        self.mock_gcs_file_system = self.gcs_file_system_patcher.start()
        self.mock_gcs_file_system.return_value = self.gcs_file_system

        self.app = Flask(__name__)

        blueprint = Blueprint("email_reporting_test", __name__)
        self.app.config["TESTING"] = True

        self.client = self.app.test_client()

        add_line_staff_tools_routes(blueprint, admin_stores)
        self.app.register_blueprint(blueprint)

        with self.app.test_request_context():
            self.state_code = StateCode.US_ID
            self.generate_emails_url = flask.url_for(
                "email_reporting_test._generate_emails",
                state_code_str=self.state_code.value,
            )

            self.send_emails_url = flask.url_for(
                "email_reporting_test._send_emails",
                state_code_str=self.state_code.value,
            )
            self.review_year = 2021
            self.review_month = 5

    def tearDown(self) -> None:
        self.gcs_file_system_patcher.stop()
        self.requires_gae_auth_patcher.stop()

    def test_generate_emails_validation(self) -> None:
        with self.app.test_request_context():
            base_json = {
                "state_code": self.state_code.value,
                "reportType": ReportType.TopOpportunities.value,
            }
            response = self.client.post(
                self.generate_emails_url,
                json={**base_json},
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                response.data,
                b"top_opportunities is not a valid ReportType",
            )

            # Invalid test address
            base_json = {
                "state_code": self.state_code.value,
                "reportType": ReportType.POMonthlyReport.value,
            }
            response = self.client.post(
                self.generate_emails_url,
                json={**base_json, "testAddress": "fake-email"},
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Invalid email address format: [fake-email]", response.data
            )

            # Invalid recipient email
            response = self.client.post(
                self.generate_emails_url,
                json={
                    "state_code": self.state_code.value,
                    "reportType": ReportType.POMonthlyReport.value,
                    "emailAllowlist": ["dev@recidiviz.org", "foo"],
                },
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(b"Invalid email address format: [foo]", response.data)

    @patch("recidiviz.reporting.email_reporting_utils.generate_batch_id")
    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_generate_emails(
        self, mock_start: MagicMock, mock_generate: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mock_generate.return_value = "test_batch_id"
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )

            response = self.client.post(
                self.generate_emails_url,
                json={
                    "state_code": self.state_code.value,
                    "reportType": ReportType.POMonthlyReport.value,
                    "emailAllowlist": ["dev@recidiviz.org", "other@recidiviz.org"],
                },
            )

            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            mock_start.assert_called_with(
                state_code=self.state_code,
                report_type=ReportType.POMonthlyReport,
                batch_id=timestamp,
                test_address=None,
                region_code=None,
                email_allowlist=["dev@recidiviz.org", "other@recidiviz.org"],
                message_body_override=None,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertIn(
                f"New batch started for {self.state_code}", str(response.data)
            )

    @patch("recidiviz.reporting.email_reporting_utils.generate_batch_id")
    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_generate_emails_with_no_allowlist(
        self, mock_start: MagicMock, mock_generate: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mock_generate.return_value = "test_batch_id"
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["letter@kenny.ca"], failures=[]
            )

            response = self.client.post(
                self.generate_emails_url,
                json={
                    "state_code": self.state_code.value,
                    "reportType": ReportType.POMonthlyReport.value,
                },
            )

            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            mock_start.assert_called_with(
                state_code=self.state_code,
                report_type=ReportType.POMonthlyReport,
                batch_id=timestamp,
                test_address=None,
                region_code=None,
                email_allowlist=None,
                message_body_override=None,
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

            response = self.client.post(
                self.generate_emails_url,
                json={
                    "state_code": self.state_code.value,
                    "reportType": ReportType.POMonthlyReport.value,
                    "emailAllowlist": ["dev@recidiviz.org", "other@recidiviz.org"],
                },
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

            response = self.client.post(
                self.generate_emails_url,
                json={
                    "state_code": self.state_code.value,
                    "reportType": ReportType.POMonthlyReport.value,
                    "emailAllowlist": ["dev@recidiviz.org", "other@recidiviz.org"],
                },
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
            response = self.client.post(
                self.generate_emails_url,
                json={
                    "state_code": self.state_code.value,
                    "reportType": ReportType.POMonthlyReport.value,
                    "emailAllowlist": ["dev@recidiviz.org", "other@recidiviz.org"],
                },
            )

            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)

    @patch("recidiviz.reporting.email_delivery.deliver")
    def test_deliver_emails_metadata(self, mock_deliver: MagicMock) -> None:
        mock_deliver.return_value = MultiRequestResult[str, str](
            successes=["dev@recidiviz.org"], failures=[]
        )

        with self.app.test_request_context():
            # Test that it works with the correct metadata
            self._upload_metadata(
                {
                    "report_type": ReportType.POMonthlyReport.value,
                    "review_year": "3000",
                    "review_month": "12",
                }
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )

            self.assertEqual(HTTPStatus.OK, response.status_code, msg=response.data)

            # Test that it fails if missing year/month
            self._upload_metadata(
                {
                    "report_type": ReportType.POMonthlyReport.value,
                    "review_month": "12",
                }
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )
            self.assertEqual(
                HTTPStatus.BAD_REQUEST, response.status_code, msg=response.data
            )

            self._upload_metadata(
                {
                    "report_type": ReportType.POMonthlyReport.value,
                    "review_year": "3000",
                }
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )

            self.assertEqual(
                HTTPStatus.BAD_REQUEST, response.status_code, msg=response.data
            )

            # Test that it 503s with an unsupported report type
            self._upload_metadata(
                {
                    "report_type": ReportType.TopOpportunities.value,
                }
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )
            self.assertEqual(
                HTTPStatus.NOT_IMPLEMENTED, response.status_code, msg=response.data
            )

    @patch("recidiviz.reporting.email_delivery.deliver")
    def test_integration_send_emails(self, mock_deliver: MagicMock) -> None:
        self._upload_metadata(
            {
                "report_type": ReportType.POMonthlyReport.value,
                "review_year": "3000",
                "review_month": "12",
            }
        )

        with self.app.test_request_context():
            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code, msg=response.data)

            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=["other@recidiviz.org"]
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )
            self.assertEqual(
                HTTPStatus.MULTI_STATUS, response.status_code, msg=response.data
            )

            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=[], failures=["dev@recidiviz.org", "other@recidiviz.org"]
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": "test_batch_id",
                    "state_code": self.state_code.value,
                },
            )
            self.assertEqual(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                response.status_code,
                msg=response.data,
            )

    def _upload_metadata(self, data: Dict[str, Any]) -> None:
        self.gcs_file_system.upload_from_string(
            path=GcsfsFilePath.from_absolute_path(
                "gs://test-project-report-html/US_ID/test_batch_id/metadata.json"
            ),
            contents=json.dumps(data),
            content_type="application/json",
        )
