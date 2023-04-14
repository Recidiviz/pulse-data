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
from typing import Optional
from unittest import TestCase
from unittest.mock import patch

import pytest

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.data_retrieval import filter_recipients, retrieve_data, start
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.reporting.recipient import Recipient
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException
from recidiviz.tools.postgres import local_postgres_helpers

FIXTURE_FILE = "po_monthly_report_data_fixture.json"


def build_report_json_fixture(email_address: str) -> str:
    return json.dumps(
        {
            "email_address": email_address,
            "state_code": "US_ID",
            "district": "DISTRICT OFFICE 3, CALDWELL",
        }
    )


@pytest.mark.uses_db
class DataRetrievalTests(TestCase):
    """Tests for reporting/data_retrieval.py."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.email_generation_patcher = patch(
            "recidiviz.reporting.email_generation.generate"
        )
        self.gcs_factory_patcher = patch(
            "recidiviz.reporting.email_reporting_handler.GcsfsFactory.build"
        )
        self.project_id_patcher.start().return_value = "recidiviz-test"
        self.mock_email_generation = self.email_generation_patcher.start()
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs

        self.get_secret_patcher = patch("recidiviz.utils.secrets.get_secret")
        self.get_secret_patcher.start()

        self.state_code = StateCode.US_ID

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.po_report_batch = Batch(
            state_code=self.state_code,
            batch_id="test-po-batch",
            report_type=ReportType.POMonthlyReport,
        )

        self.top_opp_report_batch = Batch(
            state_code=self.state_code,
            batch_id="test-top-opp-batch",
            report_type=ReportType.TopOpportunities,
        )

    def _write_test_data(self, test_data: str) -> None:
        self.fake_gcs.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-test-report-data/po_monthly_report/US_ID/po_monthly_report_data.json"
            ),
            test_data,
            "text/json",
        )

    def tearDown(self) -> None:
        self.email_generation_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_factory_patcher.stop()
        self.get_secret_patcher.stop()

        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_start(self) -> None:
        """Test that the prepared html is added to Google Cloud Storage with the correct bucket name, filepath,
        and prepared html template for the report context."""
        with open(
            os.path.join(
                f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE
            ),
            encoding="utf-8",
        ) as fixture_file:
            # Remove newlines
            self._write_test_data(json.dumps(json.loads(fixture_file.read())))

        self.mock_email_generation.side_effect = ValueError(
            "This email failed to generate!"
        )

        result = start(
            batch=self.po_report_batch,
            region_code="US_ID_D3",
            test_address="dan@recidiviz.org",
        )

        self.assertListEqual(result.failures, ["dan+letter@recidiviz.org"])
        self.assertEqual(len(result.successes), 0)

        # Email generated for recipient matching US_ID_3
        self.mock_email_generation.assert_called()

        self.mock_email_generation.reset_mock()

        start(
            batch=self.po_report_batch,
            email_allowlist=["excluded@recidiviz.org"],
        )

        # No recipients to email (none match `email_allowlist`)
        self.mock_email_generation.assert_not_called()

    def test_metadata_added(self) -> None:
        """Tests that the metadata.json file is correctly added."""
        with open(
            os.path.join(
                f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE
            ),
            encoding="utf-8",
        ) as fixture_file:
            # Remove newlines
            self._write_test_data(json.dumps(json.loads(fixture_file.read())))

        result = start(
            batch=self.po_report_batch,
            region_code="US_ID_D3",
        )
        self.assertEqual(len(result.successes), 1)

        # Test that metadata file is created correctly
        metadata_file = self.fake_gcs.download_as_string(
            GcsfsFilePath.from_absolute_path(
                f"gs://recidiviz-test-report-html/US_ID/{self.po_report_batch.batch_id}/metadata.json"
            )
        )
        self.assertEqual(
            json.loads(metadata_file),
            {
                "report_type": ReportType.POMonthlyReport.value,
                "review_month": "5",
                "review_year": "2021",
            },
        )

    def test_retrieve_data_po_monthly_report(self) -> None:
        test_data = "\n".join(
            [
                build_report_json_fixture("first@recidiviz.org"),
                "my invalid json",
                build_report_json_fixture("second@recidiviz.org"),
            ]
        )
        self._write_test_data(test_data)

        recipients = retrieve_data(batch=self.po_report_batch)

        # Invalid JSON lines are ignored; warnings are logged
        self.assertEqual(len(recipients), 2)
        self.assertEqual(recipients[0].email_address, "first@recidiviz.org")
        self.assertEqual(recipients[1].email_address, "second@recidiviz.org")

        # An archive of report JSON is stored
        self.assertEqual(
            self.fake_gcs.download_as_string(
                GcsfsFilePath.from_absolute_path(
                    f"gs://recidiviz-test-report-data-archive/{self.state_code.value}/{self.po_report_batch.batch_id}.json"
                )
            ),
            test_data,
        )

    def test_filter_recipients(self) -> None:
        dev_from_idaho = Recipient.from_report_json(
            {
                "email_address": "dev@idaho.gov",
                "state_code": "US_ID",
                "district": REGION_CODES["US_ID_D3"],
            }
        )
        dev_from_iowa = Recipient.from_report_json(
            {"email_address": "dev@iowa.gov", "state_code": "US_IA", "district": None}
        )
        recipients = [dev_from_idaho, dev_from_iowa]

        self.assertEqual(filter_recipients(recipients), recipients)
        self.assertEqual(
            filter_recipients(recipients, region_code="US_ID_D3"), [dev_from_idaho]
        )
        self.assertEqual(
            filter_recipients(
                recipients, region_code="US_ID_D3", email_allowlist=["dev@iowa.gov"]
            ),
            [],
        )
        self.assertEqual(
            filter_recipients(recipients, email_allowlist=["dev@iowa.gov"]),
            [dev_from_iowa],
        )
        self.assertEqual(
            filter_recipients(recipients, email_allowlist=["fake@iowa.gov"]), []
        )

        with self.assertRaises(InvalidRegionCodeException):
            filter_recipients(recipients, region_code="gibberish")
