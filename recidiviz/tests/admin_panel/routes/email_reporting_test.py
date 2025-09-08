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
import freezegun
import pytest
from flask import Blueprint, Flask

from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResult
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.reporting.constants import ReportType
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@pytest.mark.uses_db
class OutliersSupervisionOfficerSupervisorReportingEndpointTests(TestCase):
    """Integration tests of our flask endpoints"""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.gcs_file_system_patcher = patch(
            "recidiviz.cloud_storage.gcsfs_factory.GcsfsFactory.build"
        )
        self.gcs_file_system = FakeGCSFileSystem()
        self.mock_gcs_file_system = self.gcs_file_system_patcher.start()
        self.mock_gcs_file_system.return_value = self.gcs_file_system

        self.app = Flask(__name__)
        schema_type = SchemaType.CASE_TRIAGE
        self.database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        db_url = local_persistence_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.app, schema_type, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

        blueprint = Blueprint("email_reporting_test", __name__)
        self.app.config["TESTING"] = True

        self.client = self.app.test_client()

        add_line_staff_tools_routes(blueprint)
        self.app.register_blueprint(blueprint)

        with self.app.test_request_context():
            self.state_code = StateCode.US_PA
            self.generate_emails_url = flask.url_for(
                "email_reporting_test._generate_emails",
                state_code_str=self.state_code.value,
            )

            self.send_emails_url = flask.url_for(
                "email_reporting_test._send_emails",
                state_code_str=self.state_code.value,
            )

    def tearDown(self) -> None:
        self.gcs_file_system_patcher.stop()
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_generate_emails_validation(self) -> None:
        with self.app.test_request_context():
            base_json = {
                "state_code": self.state_code.value,
                "reportType": "POMonthlyReport",
            }
            response = self.client.post(
                self.generate_emails_url,
                json={**base_json},
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                response.data,
                b"'POMonthlyReport' is not a valid ReportType",
            )

            # Invalid test address
            base_json = {
                "state_code": self.state_code.value,
                "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
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
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
                    "emailAllowlist": ["dev@recidiviz.org", "foo"],
                },
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(b"Invalid email address format: [foo]", response.data)

    @freezegun.freeze_time(datetime.datetime(2022, 2, 25))
    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_generate_emails_for_outliers_supervisors(
        self, mock_start: MagicMock
    ) -> None:
        """Tests the generate emails endpoint"""
        with self.app.test_request_context():
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["letter@kenny.ca"], failures=[]
            )

            response = self.client.post(
                self.generate_emails_url,
                json={
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
                },
            )

            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            mock_start.assert_called_with(
                batch=Batch(
                    state_code=self.state_code,
                    batch_id=timestamp,
                    report_type=ReportType.OutliersSupervisionOfficerSupervisor,
                ),
                test_address=None,
                region_code=None,
                email_allowlist=None,
                message_body_override=None,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertIn(
                f"New batch started for {self.state_code}", str(response.data)
            )

    @patch("recidiviz.reporting.email_delivery.deliver")
    def test_deliver_emails_metadata_for_outliers_supervisors(
        self, mock_deliver: MagicMock
    ) -> None:
        mock_deliver.return_value = MultiRequestResult[str, str](
            successes=["dev@recidiviz.org"], failures=[]
        )

        with self.app.test_request_context():
            # Test that it works with the correct metadata
            self._upload_metadata(
                {
                    "report_type": ReportType.OutliersSupervisionOfficerSupervisor.value,
                }
            )

            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": timestamp,
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
                },
            )

            self.assertEqual(HTTPStatus.OK, response.status_code, msg=response.data)

    def _upload_metadata(self, data: Dict[str, Any]) -> None:
        self.gcs_file_system.upload_from_string(
            path=GcsfsFilePath.from_absolute_path(
                f"gs://test-project-report-html/{self.state_code.value}/test_batch_id/metadata.json"
            ),
            contents=json.dumps(data),
            content_type="application/json",
        )

    @patch("recidiviz.reporting.data_retrieval.start")
    def test_integration_counts_messages(
        self,
        mock_start: MagicMock,
    ) -> None:
        with self.app.test_request_context():
            mock_start.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )

            response = self.client.post(
                self.generate_emails_url,
                json={
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
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
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
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
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
                    "emailAllowlist": ["dev@recidiviz.org", "other@recidiviz.org"],
                },
            )

            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)

    @freezegun.freeze_time(datetime.datetime(2022, 2, 25))
    @patch("recidiviz.reporting.email_delivery.deliver")
    def test_integration_send_emails(self, mock_deliver: MagicMock) -> None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self._upload_metadata(
            {
                "report_type": ReportType.OutliersSupervisionOfficerSupervisor.value,
            }
        )

        with self.app.test_request_context():
            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=[]
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": timestamp,
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code, msg=response.data)

            mock_deliver.return_value = MultiRequestResult[str, str](
                successes=["dev@recidiviz.org"], failures=["other@recidiviz.org"]
            )
            response = self.client.post(
                self.send_emails_url,
                json={
                    "batchId": timestamp,
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
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
                    "batchId": timestamp,
                    "reportType": ReportType.OutliersSupervisionOfficerSupervisor.value,
                },
            )
            self.assertEqual(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                response.status_code,
                msg=response.data,
            )
