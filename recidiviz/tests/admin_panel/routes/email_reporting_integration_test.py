# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

""" Integration tests for the email pipelines """
import json
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

import flask
import freezegun
import pytest
from flask import Blueprint, Flask

from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.context.po_monthly_report.constants import Batch, ReportType
from recidiviz.reporting.email_reporting_utils import (
    get_data_filename,
    get_data_storage_bucket_name,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.reporting.email_generation_test import EmailGenerationTests
from recidiviz.tests.utils.with_secrets import with_secrets
from recidiviz.tools.postgres import local_postgres_helpers


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
@with_secrets({"po_report_cdn_static_IP": "127.0.0.1", "sendgrid_api_key": "fake"})
class EmailReportingIntegrationTests(TestCase):
    """Integration tests of our email pipelines"""

    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

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
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_postgres_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.app, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

        blueprint = Blueprint("email_reporting_test", __name__)
        self.app.config["TESTING"] = True

        self.client = self.app.test_client()

        add_line_staff_tools_routes(blueprint)
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

    def tearDown(self) -> None:
        self.requires_gae_auth_patcher.stop()
        self.gcs_file_system_patcher.stop()
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def write_fixture_to_report_data_bucket(self, batch: Batch) -> None:
        bucket = get_data_storage_bucket_name()
        filename = get_data_filename(batch)
        fixture_path = EmailGenerationTests.fixture_for_report_type(batch.report_type)

        with open(fixture_path, "r", encoding="utf-8") as fixture:
            # Convert into single-line JSON string
            report_data = json.dumps(json.loads(fixture.read()))

            self.gcs_file_system.upload_from_string(
                GcsfsFilePath.from_absolute_path(f"gs://{bucket}/{filename}"),
                report_data,
                "text/json",
            )

    @freezegun.freeze_time("2021-10-25 00:00")
    def test_integration_po_monthly(self) -> None:
        report_type = ReportType.POMonthlyReport
        batch = Batch(
            state_code=self.state_code,
            batch_id="123",
            report_type=ReportType.POMonthlyReport,
        )

        self.write_fixture_to_report_data_bucket(batch)

        response = self.client.post(
            self.generate_emails_url,
            json={
                "state_code": self.state_code.value,
                "reportType": report_type.value,
            },
        )

        self.assertEqual(response.status_code, 200)

        # The batch report data was archived
        self.assertIn(
            GcsfsFilePath(
                bucket_name="test-project-report-data-archive",
                blob_name="US_ID/20211025000000.json",
            ),
            self.gcs_file_system.all_paths,
        )

        # An email was rendered for the batch
        self.assertIn(
            GcsfsFilePath(
                bucket_name="test-project-report-html",
                blob_name="US_ID/20211025000000/html/letter@kenny.ca.html",
            ),
            self.gcs_file_system.all_paths,
        )

        # The metadata file was created
        self.assertIn(
            GcsfsFilePath(
                bucket_name="test-project-report-html",
                blob_name="US_ID/20211025000000/metadata.json",
            ),
            self.gcs_file_system.all_paths,
        )

    @freezegun.freeze_time("2021-10-25 00:00")
    @patch(
        "recidiviz.reporting.sendgrid_client_wrapper.SendGridClientWrapper.send_message"
    )
    def test_integration_overdue_discharge(self, send_message_mock: MagicMock) -> None:
        report_type = ReportType.OverdueDischargeAlert
        expected_batch = Batch(
            state_code=StateCode.US_ID,
            batch_id="20211025000000",
            report_type=report_type,
        )

        self.write_fixture_to_report_data_bucket(expected_batch)

        response = self.client.post(
            self.generate_emails_url,
            json={
                "state_code": self.state_code.value,
                "reportType": report_type.value,
            },
        )
        self.assertEqual(response.status_code, 200)

        # The batch report data was archived
        self.assertIn(
            GcsfsFilePath(
                bucket_name="test-project-report-data-archive",
                blob_name=f"US_ID/{expected_batch.batch_id}.json",
            ),
            self.gcs_file_system.all_paths,
        )

        # An email was rendered for the batch
        self.assertIn(
            GcsfsFilePath(
                bucket_name="test-project-report-html",
                blob_name=f"US_ID/{expected_batch.batch_id}/html/test@recidiviz.org.html",
            ),
            self.gcs_file_system.all_paths,
        )

        # The metadata file was created
        self.assertIn(
            GcsfsFilePath(
                bucket_name="test-project-report-html",
                blob_name=f"US_ID/{expected_batch.batch_id}/metadata.json",
            ),
            self.gcs_file_system.all_paths,
        )

        os.environ.setdefault(
            "ALERTS_FROM_EMAIL_ADDRESS", "alerts-sender@recidiviz.org"
        )
        os.environ.setdefault("ALERTS_FROM_EMAIL_NAME", "Recidiviz Alerts Sender")

        self.client.post(
            self.send_emails_url,
            json={
                "batchId": expected_batch.batch_id,
                "reportType": expected_batch.report_type.value,
            },
        )

        send_message_mock.assert_called_once()
        self.assertEqual(
            send_message_mock.call_args.kwargs["from_email"],
            "alerts-sender@recidiviz.org",
        )
        self.assertEqual(
            send_message_mock.call_args.kwargs["from_email_name"],
            "Recidiviz Alerts Sender",
        )
