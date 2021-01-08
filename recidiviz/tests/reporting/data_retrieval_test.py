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
from recidiviz.reporting.data_retrieval import start, retrieve_data, filter_recipients
from recidiviz.reporting.recipient import Recipient
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

FIXTURE_FILE = 'po_monthly_report_data_fixture.json'


def build_report_json_fixture(email_address) -> str:
    return json.dumps({
        "email_address": email_address,
        "state_code": "US_ID",
        "district": "DISTRICT OFFICE 3, CALDWELL"
    })


class EmailGenerationTests(TestCase):
    """Tests for reporting/email_generation.py."""

    def setUp(self) -> None:
        self.project_id_patcher = patch('recidiviz.utils.metadata.project_id')
        self.email_generation_patcher = patch('recidiviz.reporting.email_generation.generate')
        self.gcs_file_system_patcher = patch('recidiviz.cloud_storage.gcsfs_factory.GcsfsFactory.build')
        self.project_id_patcher.start().return_value = 'recidiviz-test'
        self.mock_email_generation = self.email_generation_patcher.start()
        self.gcs_file_system = FakeGCSFileSystem()
        self.mock_gcs_file_system = self.gcs_file_system_patcher.start()
        self.mock_gcs_file_system.return_value = self.gcs_file_system

        self.state_code = "US_ID"
        self.report_type = 'po_monthly_report'

    def _write_test_data(self, test_data):
        self.gcs_file_system.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                f'gs://recidiviz-test-report-data/po_monthly_report/{self.state_code}/po_monthly_report_data.json'
            ),
            test_data,
            "text/json"
        )

    def tearDown(self) -> None:
        self.email_generation_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_file_system_patcher.stop()

    def test_start(self) -> None:
        """Test that the prepared html is added to Google Cloud Storage with the correct bucket name, filepath,
         and prepared html template for the report context."""
        with open(os.path.join(f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE)) as fixture_file:
            # Remove newlines
            self._write_test_data(json.dumps(json.loads(fixture_file.read())))

        self.mock_email_generation.side_effect = ValueError('This email failed to generate!')

        failure_count, success_count = start(
            state_code='US_ID',
            report_type=self.report_type,
            region_code='US_ID_D3',
            test_address='dan@recidiviz.org'
        )

        self.assertEqual(failure_count, 1)
        self.assertEqual(success_count, 0)

        # Email generated for recipient matching US_ID_3
        self.mock_email_generation.assert_called()

        self.mock_email_generation.reset_mock()

        start(state_code='US_ID', report_type=self.report_type, email_allowlist=['excluded@recidiviz.org'])

        # No recipients to email (none match `email_allowlist`)
        self.mock_email_generation.assert_not_called()

    def test_retrieve_data(self) -> None:
        batch_id = "123"
        test_data = '\n'.join([
            build_report_json_fixture("first@recidiviz.org"),
            "my invalid json",
            build_report_json_fixture("second@recidiviz.org")
        ])
        self._write_test_data(test_data)

        recipients = retrieve_data(state_code=self.state_code, report_type=self.report_type, batch_id=batch_id)

        # Invalid JSON lines are ignored; warnings are logged
        self.assertEqual(len(recipients), 2)
        self.assertEqual(recipients[0].email_address, "first@recidiviz.org")
        self.assertEqual(recipients[1].email_address, "second@recidiviz.org")

        # An archive of report JSON is stored
        self.assertEqual(
            self.gcs_file_system.download_as_string(
                GcsfsFilePath.from_absolute_path(
                    'gs://recidiviz-test-report-data-archive/123.json'
                )
            ),
            test_data
        )

    def test_filter_recipients(self) -> None:
        dev_from_idaho = Recipient.from_report_json({
            'email_address': 'dev@idaho.gov', 'state_code': 'US_ID', 'district': REGION_CODES['US_ID_D3']
        })
        dev_from_iowa = Recipient.from_report_json({
            'email_address': 'dev@iowa.gov', 'state_code': 'US_IA', 'district': None
        })
        recipients = [dev_from_idaho, dev_from_iowa]

        self.assertEqual(filter_recipients(recipients), recipients)
        self.assertEqual(filter_recipients(recipients, region_code='US_ID_D3'), [dev_from_idaho])
        self.assertEqual(filter_recipients(recipients, region_code='US_ID_D3', email_allowlist=['dev@iowa.gov']), [])
        self.assertEqual(filter_recipients(recipients, email_allowlist=['dev@iowa.gov']), [dev_from_iowa])
        self.assertEqual(filter_recipients(recipients, email_allowlist=['fake@iowa.gov']), [])

        with self.assertRaises(InvalidRegionCodeException):
            filter_recipients(recipients, region_code='gibberish')
