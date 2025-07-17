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
from typing import List
from unittest import TestCase
from unittest.mock import patch

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.outliers.types import (
    OfficerSupervisorReportData,
    OutliersProductConfiguration,
)
from recidiviz.reporting.constants import ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    get_config_fixture_for_state,
)
from recidiviz.reporting.data_retrieval import filter_recipients, start
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.reporting.recipient import Recipient
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException


@pytest.mark.uses_db
class OutliersDataRetrievalTests(TestCase):
    """Tests for reporting/data_retrieval.py."""

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

        self.retrieve_data_patcher = patch(
            "recidiviz.reporting.data_retrieval.retrieve_data"
        )
        self.mock_retrieve_data = self.retrieve_data_patcher.start()

        self.state_code = StateCode.US_IX

        self.outliers_officer_supervisors = Batch(
            state_code=self.state_code,
            batch_id="test-outliers-supervisors-batch",
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
        )

    def tearDown(self) -> None:
        self.email_generation_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_factory_patcher.stop()
        self.get_secret_patcher.stop()
        self.retrieve_data_patcher.stop()


class OutliersSupervisionOfficerSupervisorDataRetrievalTest(OutliersDataRetrievalTests):
    """Class for tests specific to OutliersSupervisionOfficerSupervisor report type"""

    def config_for_state(self, state_code: StateCode) -> OutliersProductConfiguration:
        return get_config_fixture_for_state(state_code)

    @property
    def report(self) -> OfficerSupervisorReportData:
        return OfficerSupervisorReportData(
            metrics=[],
            metrics_without_outliers=[],
            recipient_email_address="recidiviz@recidiviz.org",
            additional_recipients=["additional@recidiviz.org"],
        )

    @property
    def recipients(self) -> List[Recipient]:
        return [
            Recipient(
                email_address="recidiviz@recidiviz.org",
                state_code=StateCode.US_IX,
                additional_email_addresses=["additional@recidiviz.org"],
                data={
                    "report": self.report,
                    "config": self.config_for_state(StateCode.US_IX),
                    "review_year": "2023",
                    "review_month": "7",
                },
            )
        ]

    def test_start(self) -> None:
        """Test the start() function with no failures or excpetions"""

        self.mock_retrieve_data.return_value = self.recipients

        result = start(
            batch=self.outliers_officer_supervisors,
        )

        self.assertEqual(len(result.failures), 0)
        self.assertListEqual(result.successes, ["recidiviz@recidiviz.org"])

        self.mock_email_generation.assert_called()

        self.mock_email_generation.reset_mock()
        self.mock_retrieve_data.reset_mock()

    def test_start_none_match_allowlist(self) -> None:
        """Assert generate() is not called when the recipients do not match any in the email_allowlist"""
        self.mock_retrieve_data.return_value = self.recipients

        start(
            batch=self.outliers_officer_supervisors,
            email_allowlist=["excluded@recidiviz.org"],
        )

        self.mock_email_generation.assert_not_called()

        self.mock_email_generation.reset_mock()
        self.mock_retrieve_data.reset_mock()

    def test_start_generation_failure(self) -> None:
        """Test that when an email fails to generate, it is counted in the failures."""

        self.mock_retrieve_data.return_value = [
            Recipient(
                email_address="recidiviz@recidiviz.org",
                state_code=StateCode.US_IX,
                additional_email_addresses=["additional@recidiviz.org"],
                data={
                    "report": self.report,
                    "config": self.config_for_state(StateCode.US_IX),
                    "review_year": "2023",
                    "review_month": "7",
                },
            )
        ]

        self.mock_email_generation.side_effect = ValueError(
            "This email failed to generate!"
        )

        result = start(
            batch=self.outliers_officer_supervisors,
        )

        self.assertListEqual(result.failures, ["recidiviz@recidiviz.org"])
        self.assertEqual(len(result.successes), 0)

        self.mock_email_generation.assert_called()

        self.mock_email_generation.reset_mock()
        self.mock_retrieve_data.reset_mock()

    def test_start_test_address(self) -> None:
        """
        Test that if a test address is used, the additional_email_recipients is empty in the recipients objects
        that are used in the call to generate the email.
        """

        self.mock_retrieve_data.return_value = self.recipients

        result = start(
            batch=self.outliers_officer_supervisors, test_address="test@recidiviz.org"
        )

        self.assertEqual(len(result.failures), 0)
        self.assertListEqual(result.successes, ["test+recidiviz@recidiviz.org"])

        # Assert that the Recipient argument in the generate() does not include any additional email addresses
        self.assertEqual(
            self.mock_email_generation.call_args_list[0].args[1],
            Recipient(
                email_address="test+recidiviz@recidiviz.org",
                state_code=StateCode.US_IX,
                additional_email_addresses=[],
                data={
                    "report": self.report,
                    "config": self.config_for_state(StateCode.US_IX),
                    "review_year": "2023",
                    "review_month": "7",
                },
            ),
        )

        self.mock_email_generation.reset_mock()
        self.mock_retrieve_data.reset_mock()

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
