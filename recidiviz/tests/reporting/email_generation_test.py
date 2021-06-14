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
import os
from unittest import TestCase
from unittest.mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.email_generation import generate
from recidiviz.reporting.recipient import Recipient
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

FIXTURE_FILE = "po_monthly_report_data_fixture.json"


class EmailGenerationTests(TestCase):
    """Tests for reporting/email_generation.py."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.get_secret_patcher = patch("recidiviz.utils.secrets.get_secret")
        self.gcs_file_system_patcher = patch(
            "recidiviz.reporting.email_generation.GcsfsFactory.build"
        )
        test_secrets = {"po_report_cdn_static_IP": "123.456.7.8"}
        self.get_secret_patcher.start().side_effect = test_secrets.get
        self.project_id_patcher.start().return_value = "recidiviz-test"
        self.gcs_file_system = FakeGCSFileSystem()
        self.mock_gcs_file_system = self.gcs_file_system_patcher.start()
        self.mock_gcs_file_system.return_value = self.gcs_file_system

        with open(
            os.path.join(
                f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE
            )
        ) as fixture_file:
            self.recipient = Recipient.from_report_json(json.loads(fixture_file.read()))

        self.state_code = StateCode.US_ID
        self.mock_batch_id = "1"
        self.recipient.data["batch_id"] = self.mock_batch_id
        self.report_context = PoMonthlyReportContext(self.state_code, self.recipient)

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_file_system_patcher.stop()

    def test_generate(self) -> None:
        """Test that the prepared html is added to Google Cloud Storage with the correct bucket name, filepath,
        and prepared html template for the report context."""
        html_template = self.report_context.html_template
        prepared_data = self.report_context.get_prepared_data()
        prepared_html = html_template.render(prepared_data)
        generate(self.report_context)

        bucket_name = "recidiviz-test-report-html"
        bucket_filepath = f"{self.state_code.value}/{self.mock_batch_id}/html/{self.recipient.email_address}.html"
        path = GcsfsFilePath.from_absolute_path(f"gs://{bucket_name}/{bucket_filepath}")
        self.assertEqual(self.gcs_file_system.download_as_string(path), prepared_html)

    def test_generate_incomplete_data(self) -> None:
        """Test that no files are added to Google Cloud Storage and a KeyError is raised
        if the recipient data is missing a key needed for the HTML template."""

        with self.assertRaises(KeyError):
            recipient = Recipient.from_report_json(
                {
                    "email_address": "letter@kenny.ca",
                    "state_code": "US_ID",
                    "district": "DISTRICT OFFICE 3",
                }
            )

            report_context = PoMonthlyReportContext(self.state_code, recipient)
            generate(report_context)

        self.assertEqual(self.gcs_file_system.all_paths, [])
