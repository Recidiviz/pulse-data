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
import os
import json
from string import Template

from unittest import TestCase
from unittest.mock import patch

from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.email_generation import generate

FIXTURE_FILE = 'po_monthly_report_data_fixture.json'


class EmailGenerationTests(TestCase):
    """Tests for reporting/email_generation.py."""

    def setUp(self) -> None:
        self.project_id_patcher = patch('recidiviz.utils.metadata.project_id')
        self.get_secret_patcher = patch('recidiviz.utils.secrets.get_secret')
        self.upload_string_to_storage_patcher = patch(
            'recidiviz.reporting.email_generation.utils.upload_string_to_storage')
        test_secrets = {
            'po_report_cdn_static_IP': '123.456.7.8'
        }
        self.get_secret_patcher.start().side_effect = test_secrets.get
        self.project_id_patcher.start().return_value = 'recidiviz-test'
        self.mock_upload_string_to_storage = self.upload_string_to_storage_patcher.start()

        with open(os.path.join(f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE)) as fixture_file:
            self.recipient_data = json.loads(fixture_file.read())

        self.state_code = "US_ID"
        self.mock_batch_id = 1
        self.recipient_data["batch_id"] = self.mock_batch_id
        self.report_context = PoMonthlyReportContext(self.state_code, self.recipient_data)

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.project_id_patcher.stop()
        self.upload_string_to_storage_patcher.stop()

    def test_generate(self) -> None:
        """Test that upload_string_to_storage is called with the correct bucket name, filepath, and prepared
        html template for the report context."""
        template_path = self.report_context.get_html_template_filepath()
        with open(template_path) as html_file:
            prepared_data = self.report_context.get_prepared_data()
            html_template = Template(html_file.read())
            prepared_html = html_template.substitute(prepared_data)
        generate(self.report_context)
        bucket_name = 'recidiviz-test-report-html'
        bucket_filepath = f'{self.mock_batch_id}/{self.recipient_data["email_address"]}.html'
        self.mock_upload_string_to_storage.assert_called_with(bucket_name, bucket_filepath, prepared_html, 'text/html')

    def test_generate_incomplete_data(self) -> None:
        """Test that upload_string_to_storage is not called and a KeyError is raised if the recipient data is missing
        a key needed for the HTML template."""
        with self.assertRaises(KeyError):
            self.recipient_data.pop('officer_given_name')
            report_context = PoMonthlyReportContext(self.state_code, self.recipient_data)
            generate(report_context)
            self.mock_upload_string_to_storage.assert_not_called()
